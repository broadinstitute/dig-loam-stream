package loamstream.compiler

import java.nio.file.{Files => JFiles}
import java.nio.file.Path
import java.nio.file.Paths

import loamstream.compiler.messages.ClientMessageHandler
import loamstream.compiler.messages.ErrorOutMessage
import loamstream.compiler.messages.StatusOutMessage
import loamstream.googlecloud.CloudStorageClient
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScript
import loamstream.loam.LoamToolBox
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.model.execute.Executer
import loamstream.util.Hit
import loamstream.model.jobs.{Execution, LJob}
import loamstream.util.Shot
import loamstream.util.Miss
import loamstream.util.Loggable
import loamstream.model.execute.RxExecuter
import loamstream.model.execute.Executable
import loamstream.util.StringUtils
import loamstream.conf.LoamConfig


/**
  * LoamStream
  * Created by oliverr on 7/5/2016.
  */
object LoamEngine {
  def default(
      config: LoamConfig,
      outMessageSink: ClientMessageHandler.OutMessageSink = ClientMessageHandler.OutMessageSink.NoOp,
      csClient: Option[CloudStorageClient] = None): LoamEngine = {
    
    val compiler = new LoamCompiler(LoamCompiler.Settings.default, outMessageSink)
    
    LoamEngine(config, compiler, RxExecuter.default, outMessageSink, csClient)
  }

  final case class Result(
                           projectOpt: Shot[LoamProject],
                           compileResultOpt: Shot[LoamCompiler.Result],
                           jobExecutionsOpt: Shot[Map[LJob, Execution]])

}

final case class LoamEngine(
    config: LoamConfig,
    compiler: LoamCompiler, 
    executer: Executer,
    outMessageSink: ClientMessageHandler.OutMessageSink,
    csClient: Option[CloudStorageClient] = None) extends Loggable {

  def report[T](shot: Shot[T], statusMsg: => String): Unit = {
    val message = shot match {
      case Hit(item) => StatusOutMessage(statusMsg)
      case miss: Miss => ErrorOutMessage(miss.toString)
    }
    outMessageSink.send(message)
  }

  def loadFileWithName(fileName: String): Shot[LoamScript] = loadFile(Paths.get(fileName))

  def loadFile(file: Path): Shot[LoamScript] = {
    val fileShot = {
      if (JFiles.exists(file)) {
        Hit(file)
      }
      else {
        Miss(s"Could not find '$file'.")
      }
    }
    
    import JFiles.readAllBytes
    import StringUtils.fromUtf8Bytes

    val codeShot = fileShot.flatMap(file => Shot(fromUtf8Bytes(readAllBytes(file))))

    report(codeShot, s"Loaded '$file'.")
    
    val nameShot = LoamScript.nameFromFilePath(file)

    for {
      name <- nameShot 
      code <- codeShot
    } yield LoamScript(name, code, None)
  }

  def compileFile(fileName: String): Shot[LoamCompiler.Result] = {
    val pathShot = Shot(Paths.get(fileName))

    pathShot match {
      case Hit(path) => compile(path)
      case miss: Miss =>
        outMessageSink.send(ErrorOutMessage(miss.toString))
        miss
    }
  }

  def compileFiles(files: Iterable[Path]): Shot[LoamCompiler.Result] = Shot.sequence(files.map(loadFile)).map(compile)

  def compile(file: Path): Shot[LoamCompiler.Result] = loadFile(file).map(compile)

  def compile(script: String): LoamCompiler.Result = compiler.compile(config, LoamScript.withGeneratedName(script))

  def compile(name: String, script: String): LoamCompiler.Result = {
    compiler.compile(config, LoamScript(name, script, None))
  }

  def compile(scripts: Iterable[LoamScript]): LoamCompiler.Result = compiler.compile(LoamProject(config, scripts))

  def compile(project: LoamProject): LoamCompiler.Result = compiler.compile(project)

  def compile(script: LoamScript): LoamCompiler.Result = compiler.compile(config, script)

  def compileToExecutable(code: String): Option[Executable] = {
    val compilationResult = compile(LoamScript.withGeneratedName(code))

    compilationResult.contextOpt.map(toExecutable)
  }

  def runFilesWithNames(fileNames: Iterable[String]): LoamEngine.Result = {
    val pathsShot = Shot.sequence(fileNames.map(name => Shot(Paths.get(name))))

    pathsShot match {
      case Hit(paths) => runFiles(paths)
      case miss: Miss =>
        outMessageSink.send(ErrorOutMessage(miss.toString))
        LoamEngine.Result(miss, miss, miss)
    }
  }

  def runFileWithName(fileName: String): LoamEngine.Result = {
    val pathShot = Shot {
      Paths.get(fileName)
    }
    pathShot match {
      case Hit(path) => runFile(path)
      case miss: Miss =>
        outMessageSink.send(ErrorOutMessage(miss.toString))
        LoamEngine.Result(miss, miss, miss)
    }
  }

  def runFiles(files: Iterable[Path]): LoamEngine.Result = {
    val scriptsShot = Shot.sequence(files.map(loadFile))
    scriptsShot match {
      case Hit(scripts) => run(scripts)
      case miss: Miss => LoamEngine.Result(miss, miss, miss)
    }
  }

  def runFile(file: Path): LoamEngine.Result = {
    val scriptShot = loadFile(file)
    scriptShot match {
      case Hit(script) => run(script)
      case miss: Miss => LoamEngine.Result(miss, miss, miss)
    }
  }

  private def toExecutable(context: LoamProjectContext): Executable = {
    val mapping = LoamGraphAstMapper.newMapping(context.graph)
    val toolBox = new LoamToolBox(context, csClient)

    //TODO: Remove 'addNoOpRootJob' when the executer can walk through the job graph without it
    mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _).plusNoOpRootJobIfNeeded
  }

  private def log(executable: Executable): Unit = {
    val buffer = new StringBuilder
    
    def doLog(s: String) = buffer.append(s"\n$s")
    
    executable.jobs.headOption.foreach(_.print(doPrint = doLog))
    
    debug(s"Job tree: $buffer")
  }
  
  def run(context: LoamProjectContext): Map[LJob, Execution] = {
    val executable = toExecutable(context)

    log(executable)
    
    outMessageSink.send(StatusOutMessage("Now going to execute."))

    val executions = executer.execute(executable)

    outMessageSink.send(StatusOutMessage(s"Done executing ${StringUtils.soMany(executions.size, "job")}."))

    executions
  }

  def run(code: String): LoamEngine.Result = run(LoamScript.withGeneratedName(code))

  def run(scripts: Iterable[LoamScript]): LoamEngine.Result = run(LoamProject(config, scripts))

  def run(script: LoamScript): LoamEngine.Result = run(LoamProject(config, script))

  def run(project: LoamProject): LoamEngine.Result = {
    outMessageSink.send(StatusOutMessage(s"Now compiling project with ${project.scripts.size} scripts."))
    val compileResults = compile(project)
    if (compileResults.isValid) {
      outMessageSink.send(StatusOutMessage(compileResults.summary))
      //TODO: What if compileResults.contextOpt is None?
      val context = compileResults.contextOpt.get
      val jobResults = run(context)
      LoamEngine.Result(Hit(project), Hit(compileResults), Hit(jobResults))
    } else {
      outMessageSink.send(ErrorOutMessage("Could not compile."))
      LoamEngine.Result(Hit(project), Hit(compileResults), Miss("Could not compile"))
    }
  }
}
