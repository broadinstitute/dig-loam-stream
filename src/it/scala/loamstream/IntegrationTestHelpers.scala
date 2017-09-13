package loamstream

import java.nio.file.Path
import java.nio.file.Paths

/**
 * @author clint
 * Jul 27, 2017
 */
object IntegrationTestHelpers {
  def path(s: String): Path = Paths.get(s)
  
  def withLoudStackTraces[A](f: => A): A = {
    try { f } catch {
      //NB: SBT drastically truncates stack traces. so print them manually to get more info.  
      //This workaround is lame, but gives us a chance at debugging failures.
      case e: Throwable => e.printStackTrace() ; throw e
    }
  }
}
