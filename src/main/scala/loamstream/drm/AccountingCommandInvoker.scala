package loamstream.drm

import loamstream.util.RetryingCommandInvoker
import loamstream.util.Processes

/**
 * @author clint
 * Apr 25, 2019
 */
object AccountingCommandInvoker {
  abstract class Companion[A] {
    /**
     * Make a RetryingCommandInvoker that will retrieve job metadata by running some executable.
     */
    def useActualBinary(maxRetries: Int, binaryName: String): RetryingCommandInvoker[String] = {
      def invokeBinaryFor(jobId: String) = Processes.runSync(binaryName, makeTokens(binaryName, jobId))
      
      new RetryingCommandInvoker[String](maxRetries, binaryName, invokeBinaryFor)
    }
  
    def makeTokens(actualBinary: String, jobId: String): Seq[String]
  }
}
