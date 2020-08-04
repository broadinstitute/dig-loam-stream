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
  abstract class Companion[P] extends Loggable {
    /**
     * Make a CommandInvoker that will retrieve job metadata by running some executable.
     */
    def useActualBinary(
        maxRetries: Int, 
        binaryName: String,
        scheduler: Scheduler)(implicit ec: ExecutionContext): CommandInvoker.Async[P] = {
      
      def invokeBinaryFor(param: P) = {
        val tokens = makeTokens(binaryName, param)
        
        debug(s"Invoking '$binaryName': '${tokens.mkString(" ")}'")
        
        Processes.runSync(binaryName, tokens)
      }
      
      val notRetrying = maxRetries == 0
      
      val invokeOnce = new CommandInvoker.Async.JustOnce[P](binaryName, invokeBinaryFor)
      
      if(notRetrying) {
        invokeOnce
      } else {
        new CommandInvoker.Async.Retrying[P](delegate = invokeOnce, maxRetries = maxRetries, scheduler = scheduler)
      }
    }
  
    def makeTokens(actualBinary: String, param: P): Seq[String]
  }
}
