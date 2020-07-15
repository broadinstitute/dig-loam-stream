package loamstream.drm

import scala.concurrent.ExecutionContext

import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import loamstream.util.Processes
import rx.lang.scala.Scheduler

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
        binaryName: String,
        scheduler: Scheduler)(implicit ec: ExecutionContext): CommandInvoker.Retrying[DrmTaskId] = {
      
      def invokeBinaryFor(taskId: DrmTaskId) = {
        val tokens = makeTokens(binaryName, taskId)
        
        debug(s"Invoking '$binaryName': '${tokens.mkString(" ")}'")
        
        Processes.runSync(binaryName, tokens)
      }
      
      val invokeOnce = new CommandInvoker.JustOnce[DrmTaskId](binaryName, invokeBinaryFor)
      
      new CommandInvoker.Retrying[DrmTaskId](delegate = invokeOnce, maxRetries = maxRetries, scheduler = scheduler)
    }
  
    def makeTokens(actualBinary: String, taskId: DrmTaskId): Seq[String]
  }
}
