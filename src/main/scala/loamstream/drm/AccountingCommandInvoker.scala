package loamstream.drm

import scala.concurrent.ExecutionContext

import loamstream.util.Loggable
import loamstream.util.Processes
import loamstream.util.RetryingCommandInvoker

/**
 * @author clint
 * Apr 25, 2019
 */
object AccountingCommandInvoker {
  abstract class Companion extends Loggable {
    /**
     * Make a RetryingCommandInvoker that will retrieve job metadata by running some executable.
     */
    def useActualBinary(
        maxRetries: Int, 
        binaryName: String)(implicit ec: ExecutionContext): RetryingCommandInvoker[DrmTaskId] = {
      
      def invokeBinaryFor(taskId: DrmTaskId) = {
        val tokens = makeTokens(binaryName, taskId)
        
        debug(s"Invoking '$binaryName': '${tokens.mkString(" ")}'")
        
        Processes.runSync(binaryName, tokens)
      }
      
      new RetryingCommandInvoker[DrmTaskId](maxRetries, binaryName, invokeBinaryFor)
    }
  
    def makeTokens(actualBinary: String, taskId: DrmTaskId): Seq[String]
  }
}
