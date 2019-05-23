package loamstream.apps

import scala.util.Failure
import scala.util.Success

import org.ggf.drmaa.DrmaaException

import loamstream.cli.Conf
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamProject
import loamstream.loam.LoamScript
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import loamstream.util.OneTimeLatch
import loamstream.util.Versions
import loamstream.cli.ExecutionInfo
import loamstream.cli.Intent
import loamstream.db.LoamDao
import java.nio.file.Path
import loamstream.model.execute.JobFilter
import loamstream.model.execute.DbBackedJobFilter
import loamstream.util.TimeUtils
import loamstream.model.execute.DryRunner
import loamstream.drm.DrmSystem
import loamstream.conf.LoamConfig
import org.apache.commons.io.IOUtils
import org.apache.commons.io.FileUtils
import java.nio.file.Paths
import loamstream.conf.DrmConfig
import loamstream.db.slick.DbDescriptor
import loamstream.conf.ExecutionConfig
import loamstream.conf.Locations


/**
 * @author clint
 * Nov 10, 2016
 */
object Main extends Loggable {
  def main(args: Array[String]): Unit = {
    
    val run = new Run
    
    addUncaughtExceptionHandler()
    
    val cli = Conf(args)

    describeLoamstream()

    val intent = Intent.from(cli)
    
    import Intent._
    
    intent match {
      case Right(ShowVersionAndQuit) => ()
      case Right(ShowHelpAndQuit) => cli.printHelp()
      case Right(lookup: LookupOutput) => run.doLookup(lookup)
      case Right(compileOnly: CompileOnly) => run.doCompileOnly(compileOnly)
      case Right(dryRun: DryRun) => run.doDryRun(dryRun)
      case Right(real: RealRun) => run.doRealRun(real)
      case Left(message) => cli.printHelp(message)
      case _ => cli.printHelp()
    }
  }
  
  private def addUncaughtExceptionHandler(): Unit = {
    val handler: Thread.UncaughtExceptionHandler = new Thread.UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        error(s"[${t.getName}] Fatal uncaught exception; will trigger shutdown: ", e)

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
  
      require(compilationResultShot.nonEmpty, compilationResultShot.message)
  
      compilationResultShot.get
    }
    
    def doCompileOnly(intent: Intent.CompileOnly): Unit = {
      val config = AppWiring.loamConfigFrom(intent.confFile, intent.drmSystemOpt, intent.shouldValidate)
      
      val loamEngine = LoamEngine.default(config)
      
      val compilationResult = compile(loamEngine, intent.loams)
  
      info(compilationResult.report)
    }
    
    def doDryRun(intent: Intent.DryRun, makeDao: => LoamDao = AppWiring.makeDefaultDb): Unit = {
      val config = AppWiring.loamConfigFrom(intent.confFile, intent.drmSystemOpt, intent.shouldValidate) 
      
      val jobFilter = AppWiring.jobFilterForDryRun(intent, makeDao)
      
      val loamEngine = LoamEngine.default(config)
      
      val compilationResult = compile(loamEngine, intent.loams)
  
      info(compilationResult.report)
      
      compilationResult match {
        case LoamCompiler.Result.Success(_, _, graph) => {
          val executable = LoamEngine.toExecutable(graph)
    
          val jobsToBeRun = TimeUtils.time(s"Listing jobs that would be run", info(_)) {
            DryRunner.toBeRun(jobFilter, executable)
          }
          
          info(s"Jobs to be run (${jobsToBeRun.size}):")
            
          //Log jobs that could be run normally
          jobsToBeRun.map(_.toString).foreach(info(_))
          
          //Also write them to a file, like if we were running for real.
          loamEngine.listJobsThatCouldRun(jobsToBeRun)
        }
        //Any compilaiton errors will already have been logged by LoamCompiler 
        case _ => ()
      }
    }
    
    def doLookup(intent: Intent.LookupOutput): Unit = {
      val dao = AppWiring.daoForOutputLookup(intent)
      
      val outputPathOrUri = intent.output
      
      val descriptionOpt = ExecutionInfo.forOutput(dao)(outputPathOrUri)
      
      def outputAsString: String = outputPathOrUri.fold(_.toString, _.toString)
      
      info(descriptionOpt.getOrElse(s"No records found for $outputAsString"))
    }
    
    def doRealRun(intent: Intent.RealRun, makeDao: => LoamDao = AppWiring.makeDefaultDb): Unit = {
      
      val wiring = AppWiring.forRealRun(intent, makeDao)
      
      addShutdownHook(wiring)
  
      val loamEngine = wiring.loamEngine
  
      def loamScripts: Iterable[LoamScript] = {
        val loamFiles = intent.loams
        val loamScriptsShot = loamEngine.scriptsFrom(loamFiles)
        
        require(loamScriptsShot.isHit, "Could not load loam scripts")
  
        loamScriptsShot.get
      }
      
      try {
        val project = LoamProject(loamEngine.config, loamScripts)
  
        //NB: Shut down before logging anything about jobs, so that potentially-noisy shutdown info is logged
        //before final job statuses.
        val runResults = shutdownAfter(wiring) {
          wiring.loamRunner.run(project)
        }
  
        describeRunResults(runResults)
      } catch {
        case e: DrmaaException => warn(s"Unexpected DRMAA exception: ${e.getClass.getName}", e)
      }
    }
    
    private def shutdownAfter[A](wiring: AppWiring)(f: => A): A = try { f } finally { shutdown(wiring) }
  
    private def addShutdownHook(wiring: AppWiring): Unit = {
      def toThread(block: => Any): Thread = new Thread(new Runnable { override def run: Unit = block })
      
      Runtime.getRuntime.addShutdownHook(toThread {
        shutdown(wiring)
      })
    }
    
    private def describeRunResults(runResults: Either[LoamCompiler.Result, Map[LJob, Execution]]): Unit = {
      runResults match {
        case Left(compilationResults) => {
          compilationResults.errors.foreach(e => error(s"Compilation error: $e"))
        }
        case Right(jobsToExecutions) => {
          listResults(jobsToExecutions)
      
          describeExecutions(jobsToExecutions.values)
        }
      }
    }
    
    private def listResults(jobsToExecutions: Map[LJob, Execution]): Unit = {
      //NB: Order (LJob, Execution) tuples based on the Executions' start times (if any).
      //If no start time is present (for jobs where Resources couldn't be - or weren't - 
      //determined, like Skipped jobs, those jobs/Executions come first. 
      def ordering(a: (LJob, Execution), b: (LJob, Execution)): Boolean = {
        val (_, executionA) = a
        val (_, executionB) = b
        
        (executionA.resources, executionB.resources) match {
          case (Some(resourcesA), Some(resourcesB)) => {
            resourcesA.startTime.toEpochMilli < resourcesB.startTime.toEpochMilli
          }
          case (_, None) => false
          case _ => true
        }
      }
      
      for {
        (job, execution) <- jobsToExecutions.toSeq.sortWith(ordering)
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
}
