package loamstream.apps

import loamstream.cli.Conf
import loamstream.util.Loggable
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.cli.BackendType
import org.ggf.drmaa.DrmaaException
import loamstream.util.OneTimeLatch
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.util.Versions
import scala.util.Success
import scala.util.Failure

/**
 * @author clint
 * Nov 10, 2016
 */
object Main extends Loggable {
  def main(args: Array[String]): Unit = {
    val cli = Conf(args)

    describeLoamstream()
    
    if (cli.dryRun.isSupplied) {
      compileOnly(cli)
    } else {
      run(cli)
    }
  }

  private def addShutdownHook(wiring: AppWiring): Unit = {
    def toThread(block: => Any): Thread = new Thread(new Runnable { override def run: Unit = block })
    
    Runtime.getRuntime.addShutdownHook(toThread {
      shutdown(wiring)
    })
  }

  private def outMessageSink = LoggableOutMessageSink(this)

  private def run(cli: Conf): Unit = {
    val wiring = AppWiring(cli)
    
    addShutdownHook(wiring)

    def shutdownAfter[A](f: => A): A = try { f } finally { shutdown(wiring) }

    val loamEngine = {
      val loamCompiler = new LoamCompiler(LoamCompiler.Settings.default, outMessageSink)

      LoamEngine(wiring.config, loamCompiler, wiring.executer, outMessageSink, wiring.cloudStorageClient)
    }

    try {
      //NB: Shut down before logging anything about jobs, so that potentially-noisy shutdown info is logged
      //before final job statuses.
      val engineResult = shutdownAfter {
        loamEngine.runFiles(cli.loams())
      }

      val jobsToExecutions = engineResult.jobExecutionsOpt.get
      
      listResults(jobsToExecutions)
      
      describeExecutions(jobsToExecutions.values)
    } catch {
      case e: DrmaaException => warn(s"Unexpected DRMAA exception: ${e.getClass.getName}", e)
    }
  }
  
  private def describeLoamstream(): Unit = Versions.load match {
    case Success(versions) => info(versions.toString)
    case Failure(e) => warn("Unable to determine version info: ", e)
  }

  private def listResults(jobsToExecutions: Map[LJob, Execution]): Unit = {
    for {
      (job, execution) <- jobsToExecutions
    } {
      info(s"${execution.status}\t(${execution.result}):\tRan $job got $execution")
    }
  }
  
  private def describeExecutions(executions: Iterable[Execution]): Unit = {
    def isSkipped(e: Execution) = e.status.isSkipped
      
    def allSkipped = executions.forall(isSkipped)
    
    def allSucceededOrSkipped = executions.forall(_.isSuccess) && executions.exists(isSkipped)
    
    def allSucceeded = executions.forall(_.isSuccess)
    
    def allFailed = executions.forall(_.isFailure)
    
    def someDidntFinish = executions.exists(_.status.notFinished)
    
    if(allSkipped) { info("All jobs were skipped.") } 
    else if(allSucceededOrSkipped) { info("All jobs succeeded or were skipped.") } 
    else if(allSucceeded) { info("All jobs succeeded.") } 
    else if(allFailed) { info("All jobs failed.") } 
    else if(someDidntFinish) { info("Not all jobs finished.") }
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

  private def compileOnly(cli: Conf): Unit = {
    val wiring = AppWiring(cli)

    val loamEngine = LoamEngine.default(wiring.config, outMessageSink)

    val compilationResultShot = loamEngine.compileFiles(cli.loams())

    assert(compilationResultShot.nonEmpty, compilationResultShot.message)

    val compilationResult = compilationResultShot.get

    info(compilationResult.report)
  }
}
