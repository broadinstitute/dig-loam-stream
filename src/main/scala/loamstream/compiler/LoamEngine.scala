package loamstream.compiler

import java.nio.file.{Path, Paths, Files => JFiles}

import loamstream.compiler.messages.{ClientMessageHandler, ErrorOutMessage, StatusOutMessage}
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.loam.{LoamProjectContext, LoamScript, LoamToolBox}
import loamstream.model.execute.{ChunkedExecuter, LExecuter}
import loamstream.model.jobs.LJob
import loamstream.util.{Hit, Miss, Shot, StringUtils}


/**
  * LoamStream
  * Created by oliverr on 7/5/2016.
  */
object LoamEngine {
  def default(outMessageSink: ClientMessageHandler.OutMessageSink): LoamEngine =
    LoamEngine(new LoamCompiler(LoamCompiler.Settings.default, outMessageSink),
      ChunkedExecuter.default, outMessageSink)

  final case class Result(projectOpt: Shot[LoamProject],
                          compileResultOpt: Shot[LoamCompiler.Result],
                          jobResultsOpt: Shot[Map[LJob, Shot[LJob.Result]]])

}

final case class LoamEngine(compiler: LoamCompiler, executer: LExecuter,
                            outMessageSink: ClientMessageHandler.OutMessageSink) {

  def report[T](shot: Shot[T], statusMsg: => String): Unit = {
    val message = shot match {
      case Hit(item) => StatusOutMessage(statusMsg)
      case miss: Miss => ErrorOutMessage(miss.toString)
    }
    outMessageSink.send(message)
  }

  def loadFileWithName(fileName: String): Shot[LoamScript] = {
    loadFile(Paths.get(fileName))
  }

  def loadFile(file: Path): Shot[LoamScript] = {
    val fileShot = if (JFiles.exists(file)) {
      Hit(file)
    } else {
      Miss(s"Could not find '$file'.")
    }

    import JFiles.readAllBytes

    import StringUtils.fromUtf8Bytes

    val codeShot = fileShot.flatMap(file => Shot(fromUtf8Bytes(readAllBytes(file))))

    report(codeShot, s"Loaded '$file'.")
    val nameShot = LoamScript.nameFromFilePath(file)

    for (name <- nameShot; code <- codeShot) yield LoamScript(name, code)
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

  def compile(script: String): LoamCompiler.Result = compiler.compile(LoamScript.withGeneratedName(script))

  def compile(name: String, script: String): LoamCompiler.Result = compiler.compile(LoamScript(name, script))

  def compile(scripts: Iterable[LoamScript]): LoamCompiler.Result = compiler.compile(LoamProject(scripts))

  def compile(project: LoamProject): LoamCompiler.Result = compiler.compile(project)

  def compile(script: LoamScript): LoamCompiler.Result = compiler.compile(script)

  def runFilesWithNames(fileNames: Iterable[String]): LoamEngine.Result = {
    val pathsShot = Shot.sequence(fileNames.map(name => Shot {
      Paths.get(name)
    }))
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

  def run(context: LoamProjectContext): Map[LJob, Shot[LJob.Result]] = {
    val mapping = LoamGraphAstMapper.newMapping(context.graph)
    val toolBox = new LoamToolBox(context)
    //TODO: Remove 'addNoOpRootJob' when the executer can walk through the job graph without it
    val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _).plusNoOpRootJob
    outMessageSink.send(StatusOutMessage("Now going to execute."))
    val jobResults = executer.execute(executable)
    outMessageSink.send(StatusOutMessage(s"Done executing ${StringUtils.soMany(jobResults.size, "job")}."))
    jobResults
  }

  def run(code: String): LoamEngine.Result = run(LoamScript.withGeneratedName(code))

  def run(scripts: Iterable[LoamScript]): LoamEngine.Result = run(LoamProject(scripts))

  def run(script: LoamScript): LoamEngine.Result = run(LoamProject(script))

  def run(project: LoamProject): LoamEngine.Result = {
    outMessageSink.send(StatusOutMessage(s"Now compiling project with ${project.scripts.size} scripts."))
    val compileResults = compile(project)
    if (!compileResults.isValid) {
      outMessageSink.send(ErrorOutMessage("Could not compile."))
      LoamEngine.Result(Hit(project), Miss("Could not compile"), Miss("Could not compile"))
    } else {
      outMessageSink.send(StatusOutMessage(compileResults.summary))
      val context = compileResults.contextOpt.get
      val jobResults = run(context)
      LoamEngine.Result(Hit(project), Hit(compileResults), Hit(jobResults))
    }
  }

}
