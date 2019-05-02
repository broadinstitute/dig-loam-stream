package loamstream.drm

import loamstream.util.RetryingCommandInvoker
import loamstream.util.Processes
import loamstream.util.Loggable

/**
 * @author clint
 * Apr 25, 2019
 */
object AccountingCommandInvoker {
  abstract class Companion[A] extends Loggable {
    /**
     * Make a RetryingCommandInvoker that will retrieve job metadata by running some executable.
     */
    def useActualBinary(maxRetries: Int, binaryName: String): RetryingCommandInvoker[String] = {
      def invokeBinaryFor(jobId: String) = {
        val tokens = makeTokens(binaryName, jobId)
        
        debug(s"Invoking '$binaryName': '${tokens.mkString(" ")}'")
        
        Processes.runSync(binaryName, tokens)
      }
      
      new RetryingCommandInvoker[String](maxRetries, binaryName, invokeBinaryFor)
    }
  
    def makeTokens(actualBinary: String, jobId: String): Seq[String]
  }
}
