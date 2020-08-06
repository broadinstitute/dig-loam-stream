package loamstream.drm

import scala.util.Try
import loamstream.util.RunResults
import loamstream.util.Processes
import loamstream.util.Users
import loamstream.util.Loggable
import loamstream.util.CommandInvoker
import loamstream.conf.DrmConfig
import scala.util.Success

/**
 * @author clint
 * Jul 14, 2020
 */
trait JobKiller {
  def killAllJobs(): Unit
}

object JobKiller extends Loggable {
  abstract class Companion[A](
      defaultExecutable: String,
      constructor: (CommandInvoker.Sync[Unit], SessionSource) => A) {
    
    protected def makeTokens(sessionSource: SessionSource, actualExecutable: String, username: String): Seq[String]
    
    def fromExecutable(
        sessionSource: SessionSource,
        config: DrmConfig,
        actualExecutable: String = defaultExecutable,
        username: String = Users.currentUser,
        isSuccess: Int => Boolean): A = {
        
      val killJobs: Unit => Try[RunResults] = { _ =>
        val tokens = makeTokens(sessionSource, actualExecutable, username)
        
        debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
        
        import RunResults._
        
        Processes.runSync(tokens)(isSuccess = isSuccess)
      }
      
      val justOnce = new CommandInvoker.Sync.JustOnce[Unit](actualExecutable, killJobs)
      
      val commandInvoker = new CommandInvoker.Sync.Retrying(justOnce, maxRetries = config.maxRetries)
      
      constructor(commandInvoker, sessionSource)
    }
  }
}
