package loamstream.drm

import scala.concurrent.ExecutionContext

import loamstream.util.Loggable
import loamstream.util.Processes
import loamstream.util.RetryingCommandInvoker
import rx.lang.scala.Scheduler
import scala.util.Try
import loamstream.util.RunResults

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
        scheduler: Scheduler)
       (implicit ec: ExecutionContext): RetryingCommandInvoker[Either[DrmTaskId, DrmTaskArray]] = {
      
      def invokeBinaryFor(taskIdOrArray: Either[DrmTaskId, DrmTaskArray]): Try[RunResults] = {
        val tokens = makeTokens(binaryName, taskIdOrArray)
        
        debug(s"Invoking '$binaryName': '${tokens.mkString(" ")}'")
        
        val result = Processes.runSync(binaryName, tokens)
        
        result.foreach(_.logStdOutAndStdErr("FIXME: qacct output:"))
        
        result.recover {
          case e => error(s"FIXME: Error invoking qacct: $e", e)
        }
        
        result
      }
      
      new RetryingCommandInvoker[Either[DrmTaskId, DrmTaskArray]](
          maxRetries, 
          binaryName, 
          invokeBinaryFor, 
          scheduler = scheduler)
    }
  
    def makeTokens(actualBinary: String, taskId: Either[DrmTaskId, DrmTaskArray]): Seq[String]
  }
}
