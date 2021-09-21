package loamstream.drm

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import loamstream.conf.DrmConfig
import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import loamstream.util.Processes
import loamstream.util.RunResults
import loamstream.util.Users
import loamstream.util.OneTimeLatch

/**
 * @author clint
 * Jul 14, 2020
 */
trait JobKiller {
  private val latch = new OneTimeLatch

  final def killAllJobs(): Unit = latch.doOnce(doKillAllJobs())

  protected def doKillAllJobs(): Unit
}

object JobKiller extends Loggable {
  abstract class ForCommand(
      drmSystem: DrmSystem,
      commandInvoker: CommandInvoker.Sync[Unit], 
      sessionTracker: SessionTracker) extends JobKiller with Loggable {

    override protected def doKillAllJobs(): Unit = {
      if(sessionTracker.nonEmpty) {
        commandInvoker.apply(()) match {
          case Success(runResults) => debug(s"Killed ${drmSystem.name} jobs")
          case Failure(e) => warn(s"Error killing all ${drmSystem.name} jobs: ${e.getMessage}", e)
        }
      } else {
        debug(s"No ${drmSystem.name} session initialized; not killing jobs")
      }
    }
  }
  
  abstract class Companion[A](
      defaultExecutable: String,
      constructor: (CommandInvoker.Sync[Unit], SessionTracker) => A) {
    
    protected def makeTokens(sessionTracker: SessionTracker, actualExecutable: String, username: String): Seq[String]
    
    def fromExecutable(
        sessionTracker: SessionTracker,
        config: DrmConfig,
        actualExecutable: String = defaultExecutable,
        username: String = Users.currentUser,
        isSuccess: RunResults.SuccessPredicate = RunResults.SuccessPredicate.zeroIsSuccess): A = {
        
      val killJobs: Unit => Try[RunResults] = _ => Try {
        val tokens = makeTokens(sessionTracker, actualExecutable, username)
        
        debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
        
        import RunResults._
        
        Processes.runSync(tokens)()
      }
      
      val justOnce = new CommandInvoker.Sync.JustOnce[Unit](actualExecutable, killJobs, isSuccess = isSuccess)
      
      val commandInvoker = new CommandInvoker.Sync.Retrying(justOnce, maxRetries = config.maxRetries)
      
      constructor(commandInvoker, sessionTracker)
    }
  }
}
