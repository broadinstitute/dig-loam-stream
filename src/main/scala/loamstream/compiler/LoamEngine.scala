package loamstream.compiler

import java.nio.file.{Path, Paths, Files => JFiles}

import loamstream.compiler.messages.{ClientMessageHandler, ErrorOutMessage, StatusOutMessage}
import loamstream.loam.{LoamContext, LoamToolBox}
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.model.execute.Executer
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob
import loamstream.util.{Hit, Miss, Shot, StringUtils}


/**
  * LoamStream
  * Created by oliverr on 7/5/2016.
  */
object LoamEngine {
  def default(outMessageSink: ClientMessageHandler.OutMessageSink): LoamEngine =
    LoamEngine(new LoamCompiler(outMessageSink), RxExecuter.default, outMessageSink)

  final case class Result(sourceCodeOpt: Shot[String],
                          compileResultOpt: Shot[LoamCompiler.Result],
                          jobResultsOpt: Shot[Map[LJob, Shot[JobState]]])

}

final case class LoamEngine(compiler: LoamCompiler, executer: Executer, 
                            outMessageSink: ClientMessageHandler.OutMessageSink) {

  def report[T](shot: Shot[T], statusMsg: => String): Unit = {
    val message = shot match {
      case Hit(item) => StatusOutMessage(statusMsg)
      case miss: Miss => ErrorOutMessage(miss.toString)
    }
    outMessageSink.send(message)
  }

  def loadFile(fileName: String): Shot[String] = {
    loadFile(Paths.get(fileName))
  }

  def loadFile(file: Path): Shot[String] = {
    import JFiles.readAllBytes
    import StringUtils.fromUtf8Bytes
    val fileShot = if (JFiles.exists(file)) {
      Hit(file)
    } else if (!file.toString.endsWith(".loam")) {
      val alternateFile = Paths.get(file.toString + ".loam")
      if (JFiles.exists(alternateFile)) {
        Hit(alternateFile)
      } else {
        Miss(s"Could not find '$file' nor '$alternateFile'.")
      }
    } else {
      Miss(s"Could not find '$file'.")
    }

    val scriptShot = fileShot.flatMap(file => Shot(fromUtf8Bytes(readAllBytes(file))))

    report(scriptShot, s"Loaded '$file'.")

    scriptShot
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

  def compile(file: Path): Shot[LoamCompiler.Result] = loadFile(file).map(compile)

  def compile(script: String): LoamCompiler.Result = compiler.compile(script)

  def runFile(fileName: String): LoamEngine.Result = {
    val pathShot = Shot {
      Paths.get(fileName)
    }
    pathShot match {
      case Hit(path) => run(path)
      case miss: Miss =>
        outMessageSink.send(ErrorOutMessage(miss.toString))
        LoamEngine.Result(miss, miss, miss)
    }
  }

  def run(file: Path): LoamEngine.Result = {
    val scriptShot = loadFile(file)
    scriptShot match {
      case Hit(script) => run(script)
      case miss: Miss => LoamEngine.Result(miss, miss, miss)
    }

  }

  def run(context: LoamContext): Map[LJob, Shot[JobState]] = {
    val mapping = LoamGraphAstMapper.newMapping(context.graph)
    val toolBox = new LoamToolBox(context)
    //TODO: Remove 'addNoOpRootJob' when the executer can walk through the job graph without it
    val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _).plusNoOpRootJob
    outMessageSink.send(StatusOutMessage("Now going to execute."))
    val jobResults = executer.execute(executable)
    outMessageSink.send(StatusOutMessage(s"Done executing ${StringUtils.soMany(jobResults.size, "job")}."))
    jobResults
  }

  def run(script: String): LoamEngine.Result = {
    outMessageSink.send(StatusOutMessage(s"Now compiling script of ${script.length} characters."))
    val compileResults = compile(script)
    if (!compileResults.isValid) {
      outMessageSink.send(ErrorOutMessage("Could not compile."))
      LoamEngine.Result(Hit(script), Miss("Could not compile"), Miss("Could not compile"))
    } else {
      outMessageSink.send(StatusOutMessage(compileResults.summary))
      val context = compileResults.contextOpt.get
      val jobResults = run(context)
      LoamEngine.Result(Hit(script), Hit(compileResults), Hit(jobResults))
    }
  }

}
