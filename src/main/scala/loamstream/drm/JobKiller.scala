package loamstream.drm

import scala.util.Try
import loamstream.util.RunResults
import loamstream.util.Processes
import loamstream.util.Users

/**
 * @author clint
 * Jul 14, 2020
 */
trait JobKiller {
  def killAllJobs(): Unit
}

object JobKiller {
  type InvocationFn = () => Try[RunResults]
  
  abstract class Companion[A](
      defaultExecutable: String,
      constructor: InvocationFn => A) {
    
    protected def makeTokens(sessionSource: SessionSource, actualExecutable: String, username: String): Seq[String]
    
    def fromExecutable(
        sessionSource: SessionSource,
        actualExecutable: String = defaultExecutable,
        username: String = Users.currentUser): A = {
        
      val killJobs: InvocationFn = { () =>
        Processes.runSync(makeTokens(sessionSource, actualExecutable, username))
      }
      
      constructor(killJobs)
    }
  }
}
