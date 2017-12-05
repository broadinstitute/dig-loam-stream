package loamstream.compiler

import java.nio.file.{ Files => JFiles }
import java.nio.file.Path
import java.nio.file.Paths

import loamstream.conf.LoamConfig
import loamstream.googlecloud.CloudStorageClient
import loamstream.loam.LoamGraph
import loamstream.loam.LoamScript
import loamstream.loam.LoamToolBox
import loamstream.model.execute.Executable
import loamstream.model.execute.Executer
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.LJob
import loamstream.util.Hit
import loamstream.util.Loggable
import loamstream.util.Miss
import loamstream.util.Shot
import loamstream.util.StringUtils


/**
  * LoamStream
  * Created by oliverr on 7/5/2016.
  */
object LoamEngine {
  def default(
      config: LoamConfig,
      csClient: Option[CloudStorageClient] = None): LoamEngine = {
    
    val compiler = LoamCompiler.default
    
    LoamEngine(config, compiler, RxExecuter.default, csClient)
  }

  final case class Result(
      projectOpt: Shot[LoamProject],
      compileResultOpt: Shot[LoamCompiler.Result],
      jobExecutionsOpt: Shot[Map[LJob, Execution]])
      
  def toExecutable(graph: LoamGraph, csClient: Option[CloudStorageClient] = None): Executable = {
    
    val toolBox = new LoamToolBox(csClient)

    toolBox.createExecutable(graph)
  }
}

final case class LoamEngine(
    config: LoamConfig,
    compiler: LoamCompiler, 
    executer: Executer,
    csClient: Option[CloudStorageClient] = None) extends Loggable {

  def report[T](shot: Shot[T], statusMsg: => String): Unit = {
    shot match {
      case Hit(item) => info(statusMsg)
      case miss: Miss => error(miss.toString)
    }
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
    
    import java.nio.file.Files.readAllBytes
    import loamstream.util.StringUtils.fromUtf8Bytes

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
      case miss: Miss => {
        error(miss.toString)
        miss
      }
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

    compilationResult.contextOpt.map(_.graph).map(toExecutable)
  }

  def runFilesWithNames(fileNames: Iterable[String]): LoamEngine.Result = {
    val pathsShot = Shot.sequence(fileNames.map(name => Shot(Paths.get(name))))

    pathsShot match {
      case Hit(paths) => runFiles(paths)
      case miss: Miss => {
        error(miss.toString)
        LoamEngine.Result(miss, miss, miss)
      }
    }
  }

  def runFileWithName(fileName: String): LoamEngine.Result = {
    val pathShot = Shot(Paths.get(fileName))

    pathShot match {
      case Hit(path) => runFile(path)
      case miss: Miss => {
        error(miss.toString)
        LoamEngine.Result(miss, miss, miss)
      }
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

  private def toExecutable(graph: LoamGraph): Executable = LoamEngine.toExecutable(graph, csClient)
  
  def run(graph: LoamGraph): Map[LJob, Execution] = {
    info("Making Executable from LoamGraph")
    
    val executable = toExecutable(graph)
    
    info("Now going to execute.")

    val executions = executer.execute(executable)

    info(s"Done executing ${StringUtils.soMany(executions.size, "job")}.")

    executions
  }

  def run(code: String): LoamEngine.Result = run(LoamScript.withGeneratedName(code))

  def run(scripts: Iterable[LoamScript]): LoamEngine.Result = run(LoamProject(config, scripts))

  def run(script: LoamScript): LoamEngine.Result = run(LoamProject(config, script))

  def run(project: LoamProject): LoamEngine.Result = {
    info(s"Now compiling project with ${project.scripts.size} scripts.")
    
    val compileResults = compile(project)
    
    if (compileResults.isValid) {
      info(compileResults.summary)
      //TODO: What if compileResults.contextOpt is None?
      val context = compileResults.contextOpt.get
      val jobResults = run(context.graph)
      LoamEngine.Result(Hit(project), Hit(compileResults), Hit(jobResults))
    } else {
      error("Could not compile.")
      LoamEngine.Result(Hit(project), Hit(compileResults), Miss("Could not compile"))
    }
  }
  
  def scriptsFrom(files: Iterable[Path]): Shot[Iterable[LoamScript]] = Shot.sequence(files.map(loadFile))
}
