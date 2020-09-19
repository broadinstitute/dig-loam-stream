package loamstream.apps

import java.nio.file.Path

import scala.util.Failure
import scala.util.Success

import loamstream.cli.Conf
import loamstream.cli.Intent
import loamstream.cli.JobFilterIntent
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamProject
import loamstream.conf.LoamConfig
import loamstream.conf.LsSettings
import loamstream.db.LoamDao
import loamstream.loam.LoamScript
import loamstream.model.execute.DryRunner
import loamstream.model.execute.Run
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import loamstream.util.OneTimeLatch
import loamstream.util.TimeUtils
import loamstream.util.Versions


/**
 * @author clint
 * Nov 10, 2016
 */
object Main extends Loggable {
  def main(args: Array[String]): Unit = {
    
    addUncaughtExceptionHandler(None)
    
    val cli = Conf(args)

    describeLoamstream()

    info(s"Worker mode is ${if(cli.toValues.workerSupplied) "ON" else "OFF"}")
    
    val intent = Intent.from(cli)
    
    import loamstream.cli.Intent._

    def run = new Run
    
    intent match {
      case Right(ShowVersionAndQuit) => ()
      case Right(ShowHelpAndQuit) => cli.printHelp()
      case Right(compileOnly: CompileOnly) => run.doCompileOnly(compileOnly)
      case Right(dryRun: DryRun) => run.doDryRun(dryRun)()
      case Right(real: RealRun) => run.doRealRun(real)
      case Left(message) => cli.printHelp(message)
      case _ => cli.printHelp()
    }
  }
  
  private def addUncaughtExceptionHandler(wiringOpt: Option[AppWiring] = None): Unit = {
    val handler: Thread.UncaughtExceptionHandler = new Thread.UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        error(s"[${t.getName}] Fatal uncaught exception; will trigger shutdown: ", e)

        wiringOpt.foreach(shutdown)
        
        e.printStackTrace(System.err)
      }
    }
    
    Thread.setDefaultUncaughtExceptionHandler(handler)
    
    Thread.currentThread.setUncaughtExceptionHandler(handler)
  }

  private def describeLoamstream(): Unit = Versions.load match {
    case Success(versions) => info(versions.toString)
    case Failure(e) => warn("Unable to determine version info: ", e)
  }
  
  private[apps] final class Run extends Loggable {
    
    private def compile(loamEngine: LoamEngine, loams: Seq[Path]): LoamCompiler.Result = {
      val compilationResultShot = loamEngine.compileFiles(loams)
  
      require(compilationResultShot.isSuccess, compilationResultShot.failed.get.getMessage)
  
      compilationResultShot.get
    }
    
    def doCompileOnly(intent: Intent.CompileOnly): Unit = {
      val config = {
        AppWiring.loamConfigFrom(intent.confFile, intent.drmSystemOpt, intent.shouldValidate, intent.cliConfig)
      }
      
      val lsSettings = LsSettings(intent.cliConfig.map(_.toValues))
      
      val loamEngine = LoamEngine.default(config, lsSettings)
      
      val compilationResult = compile(loamEngine, intent.loams)
  
      info(compilationResult.report)
    }
    
    def doDryRun(
        intent: Intent.DryRun)( 
        makeDao: LoamConfig => LoamDao = AppWiring.makeDefaultDb): Unit = {
      
      val config = {
        AppWiring.loamConfigFrom(intent.confFile, intent.drmSystemOpt, intent.shouldValidate, intent.cliConfig)
      }
      
      val lsSettings = LsSettings(intent.cliConfig.map(_.toValues))
      
      val loamEngine = LoamEngine.default(config, lsSettings)
      
      val compilationResult = compile(loamEngine, intent.loams)
  
      info(compilationResult.report)
      
      compilationResult match {
        case LoamCompiler.Result.Success(_, _, graph) => {
          val executable = LoamEngine.toExecutable(graph, config.executionConfig)
    
          val jobsToBeRun = intent.jobFilterIntent match {
            case JobFilterIntent.AsByNameJobFilter(byNameFilter) => DryRunner.toBeRun(byNameFilter, executable) 
            case _ => {
              val jobFilter = AppWiring.jobFilterForDryRun(intent, config, makeDao)
              
              DryRunner.toBeRun(jobFilter, executable)
            }
          }
          
          info(s"Jobs that COULD run (${jobsToBeRun.size}):")
            
          //Log jobs that could be run normally
          jobsToBeRun.map(j => s"(name: '${j.name}') $j").foreach(info(_))
          
          info(s"Done listing ${jobsToBeRun.size} jobs that COULD run.")
          
          //Also write them to a file, like if we were running for real.
          loamEngine.listJobsThatCouldRun(jobsToBeRun)
        }
        //Any compilaiton errors will already have been logged by LoamCompiler 
        case _ => ()
      }
    }
    
    def doRealRun(
        intent: Intent.RealRun, 
        makeDao: LoamConfig => LoamDao = conf => AppWiring.makeDefaultDbIn(conf.executionConfig.dbDir)): Unit = {
      
      val wiring = AppWiring.forRealRun(intent, makeDao)
      
      val lsDir =  wiring.config.executionConfig.loamstreamDir.toAbsolutePath
      
      info(s"Loamstream will create logs and metadata files under ${lsDir}")
      
      addShutdownHook(wiring)
      
      addUncaughtExceptionHandler(Some(wiring))
  
      val loamEngine = wiring.loamEngine
  
      def loamScripts: Iterable[LoamScript] = {
        val loamFiles = intent.loams
        val loamScriptsAttempt = loamEngine.scriptsFrom(loamFiles)
        
        require(loamScriptsAttempt.isSuccess, "Could not load loam scripts")
  
        loamScriptsAttempt.get
      }
      
      try {
        val project = LoamProject(loamEngine.config, wiring.settings, loamScripts)
  
        //NB: Shut down before logging anything about jobs, so that potentially-noisy shutdown info is logged
        //before final job statuses.
        val (runResults, elapsedMillis) = shutdownAfter(wiring) {
          wiring.dao.registerNewRun(Run.create())
          
          TimeUtils.elapsed {
            wiring.loamRunner.run(project)
          }
        }
        
        describeRunResults(loamEngine.config, runResults.get)
        
        info(s"Running project with ${project.scripts.size} scripts took ${elapsedMillis / 1000} seconds")
      } finally {
        shutdown(wiring)
      }
    }
    
    private def shutdownAfter[A](wiring: AppWiring)(f: => A): A = try { f } finally { shutdown(wiring) }
  
    private def addShutdownHook(wiring: AppWiring): Unit = {
      def toThread(block: => Any): Thread = new Thread(new Runnable { override def run: Unit = block })
      
      Runtime.getRuntime.addShutdownHook(toThread {
        shutdown(wiring)
      })
    }
    
    private def describeRunResults(
        config: LoamConfig, 
        runResults: Either[LoamCompiler.Result, Map[LJob, Execution]]): Unit = runResults match {

      case Left(compilationResults) => compilationResults.errors.foreach(e => error(s"Compilation error: $e"))
      case Right(jobsToExecutions) => {
        listResults(jobsToExecutions)
        
        IndexFiles.writeIndexFiles(config.executionConfig, jobsToExecutions)
    
        describeExecutions(jobsToExecutions.values)
      }
    }
    
    private def listResults(jobsToExecutions: Map[LJob, Execution]): Unit = {
      for {
        (job, execution) <- jobsToExecutions.toSeq.sortWith(Orderings.executionTupleOrdering)
      } {
        info(s"${execution.status}\t(${execution.result}):\tRan $job got $execution")
      }
    }
    
    private def describeExecutions(executions: Iterable[Execution]): Unit = {
      def isSkipped(e: Execution) = e.status.isSkipped
      def isCouldNotStart(e: Execution) = e.status.isCouldNotStart
      def neitherSuccessNorFailure(e: Execution) = !e.isSuccess && !e.isFailure
        
      val numSucceeded = executions.count(_.isSuccess)
      val numFailed = executions.count(_.isFailure)
      val numSkipped = executions.count(isSkipped)
      val numCouldNotStart = executions.count(isCouldNotStart)
      val numOther = executions.count(neitherSuccessNorFailure)
      val numRan = executions.size
      
      val message = {
        s"$numRan jobs ran. $numSucceeded succeeded, $numFailed failed, $numSkipped skipped, " + 
        s"$numCouldNotStart could not start, $numOther other."
      }
      
      info(message)
    }
  }
  
  private[this] val shutdownLatch: OneTimeLatch = new OneTimeLatch
  
  private[apps] def shutdown(wiring: AppWiring): Unit = {
    shutdownLatch.doOnce {
      info("LoamStream shutting down...")
      
      wiring.shutdown() match {
        case Nil => info("LoamStream shut down successfully")
        case exceptions => {
          error(s"LoamStream shut down with ${exceptions.size} errors: ")

          exceptions.foreach { e =>
            error(s"Error shutting down: ${e.getClass.getName}", e)
          }
        }
      }
    }
  }
}
