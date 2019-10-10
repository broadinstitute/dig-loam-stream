package loamstream.compiler

import java.io.FileWriter
import java.io.PrintWriter
import java.nio.file.{ Files => JFiles }
import java.nio.file.Path
import java.time.Instant

import scala.util.control.NonFatal

import loamstream.conf.LoamConfig
import loamstream.googlecloud.CloudStorageClient
import loamstream.loam.LoamGraph
import loamstream.loam.LoamScript
import loamstream.loam.LoamToolBox
import loamstream.model.execute.DryRunner
import loamstream.model.execute.Executable
import loamstream.model.execute.Executer
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.HasCommandLine
import loamstream.util.CanBeClosed
import loamstream.util.Hit
import loamstream.util.IoUtils
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
    
    val compiler = LoamCompiler(config.compilationConfig, LoamCompiler.Settings.default) 
    
    LoamEngine(config, compiler, RxExecuter.default, csClient)
  }

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

    codeShot match {
      case Hit(_) => info(s"Loaded '$file'.")
      case miss: Miss => error(miss.toString)
    }
    
    val nameShot = LoamScript.nameFromFilePath(file)

    for {
      name <- nameShot 
      code <- codeShot
    } yield LoamScript(name, code, None)
  }

  def compileFiles(files: Iterable[Path]): Shot[LoamCompiler.Result] = {
    def compileScripts(scripts: Iterable[LoamScript]): LoamCompiler.Result = {
      compiler.compile(LoamProject(config, scripts))
    }
    
    Shot.sequence(files.map(loadFile)).map(compileScripts)
  }

  def compile(project: LoamProject): LoamCompiler.Result = {
    info(s"Now compiling project with ${project.scripts.size} scripts.")
    
    compiler.compile(project)
  }
  
  def run(graph: LoamGraph): Map[LJob, Execution] = {
    info("Making Executable from LoamGraph")
    
    val executable = LoamEngine.toExecutable(graph, csClient)
    
    listJobsThatCouldRun(executable, config.executionConfig.dryRunOutputFile)
    
    info("Now going to execute.")

    val jobOracle = JobOracle.fromExecutable(config.executionConfig, executable)
    
    try {
      val executions = executer.execute(executable, _ => jobOracle)
  
      info(s"Done executing ${StringUtils.soMany(executions.size, "job")}.")
  
      executions
    } catch {
      case NonFatal(e) => {
        error(s"Caught exception when running, attempting to provide information about jobs for debugging.", e)
        
        makeExecutionMapInCaseOfException(executable, jobOracle)
      }
    }
  }
  
  private def makeExecutionMapInCaseOfException(executable: Executable, jobOracle: JobOracle): Map[LJob, Execution] = {
    def getCommand(j: LJob): Option[String] = j match {
      case clj: HasCommandLine => Some(clj.commandLineString)
      case _ => None
    }
    
    executable.allJobs.map { job =>
      val e = Execution(
          cmd = getCommand(job),
          settings = job.initialSettings,
          status = JobStatus.Unknown,
          result = None,
          resources = None,
          outputs = job.outputs.map(_.toStoreRecord),
          jobDir = jobOracle.dirOptFor(job),
          terminationReason = None)
      
      job -> e
    }.toMap
  }
  
  private def listJobsThatCouldRun(executable: Executable, jobListOutputFile: Path): Unit = {
    listJobsThatCouldRun(DryRunner.toBeRun(executer.jobFilter, executable), jobListOutputFile)
  }
  
  //TODO: Find a good place for this; it's exposed so it can be called from here and loamstream.apps.Main
  def listJobsThatCouldRun(
      jobsThatCouldRun: => Iterable[LJob], 
      outputFile: Path = config.executionConfig.dryRunOutputFile): Unit = {
    
    lazy val jobs = jobsThatCouldRun
    
    val append = true
    
    CanBeClosed.enclosed(new PrintWriter(new FileWriter(outputFile.toFile, append))) { writer =>
      info(s"Listing (${jobs.size}) jobs that could be run to ${outputFile}")
      
      for { 
        job <- jobs 
      } {
        //NB: Don't log using SLF4J, since it's hard to make that happen such that the file where log messages end up
        //is (conveniently) configurable at runtime, say via CLI params that we control (and aren't -D) and not by
        //messing with `logback.xml`.
        //TODO: Use SLF4J somehow.
        IoUtils.printTo(writer)(s"[${Instant.now}] $job")
      }
    }
  }

  def scriptsFrom(files: Iterable[Path]): Shot[Iterable[LoamScript]] = Shot.sequence(files.map(loadFile))
}
