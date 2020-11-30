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
import loamstream.util.IoUtils
import loamstream.util.Loggable
import loamstream.util.StringUtils
import scala.util.Try
import scala.util.Success
import loamstream.util.Tries
import scala.util.Failure
import loamstream.loam.LoamLoamScript
import loamstream.loam.ScalaLoamScript
import loamstream.util.Files
import loamstream.conf.LsSettings
import loamstream.conf.ExecutionConfig
import loamstream.util.TimeUtils


/**
  * LoamStream
  * Created by oliverr on 7/5/2016.
  */
object LoamEngine extends Loggable {
  def default(
      config: LoamConfig,
      settings: LsSettings,
      csClient: Option[CloudStorageClient] = None): LoamEngine = {
    
    val compiler = LoamCompiler(config.compilationConfig, LoamCompiler.Settings.default) 
    
    LoamEngine(config, settings, compiler, RxExecuter.default, csClient)
  }

  def toExecutable(
      graph: LoamGraph, 
      executionConfig: ExecutionConfig = ExecutionConfig.default, 
      csClient: Option[CloudStorageClient] = None): Executable = {
    
    val toolBox = new LoamToolBox(csClient)

    TimeUtils.time("Making Executable", debug(_)) {
      debug("Making Executable...")
      
      toolBox.createExecutable(graph, executionConfig)
    }
  }
}

final case class LoamEngine(
    config: LoamConfig,
    settings: LsSettings,
    compiler: LoamCompiler, 
    executer: Executer,
    csClient: Option[CloudStorageClient] = None) extends Loggable {

  def loadFile(file: Path): Try[LoamScript] = LoamScript.read(file)

  def compileFiles(files: Iterable[Path]): Try[LoamCompiler.Result] = {
    def compileScripts(scripts: Iterable[LoamScript]): LoamCompiler.Result = {
      compile(LoamProject(config, settings, scripts))
    }
    
    Tries.sequence(files.map(loadFile)).map(compileScripts)
  }

  def compile(project: LoamProject): LoamCompiler.Result = {
    info(s"Now compiling project with ${project.scripts.size} scripts.")
    
    compiler.compile(project)
  }
  
  def run(graph: LoamGraph, executionConfig: ExecutionConfig = ExecutionConfig.default): Map[LJob, Execution] = {
    info("Making Executable from LoamGraph")
    
    val executable = LoamEngine.toExecutable(graph, executionConfig, csClient)
    
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
        
        executer.stop()
        
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

  def scriptsFrom(files: Iterable[Path]): Try[Iterable[LoamScript]] = Tries.sequence(files.map(loadFile))
}
