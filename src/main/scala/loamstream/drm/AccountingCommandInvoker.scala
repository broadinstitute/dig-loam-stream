package loamstream.drm

import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import loamstream.util.Processes
import monix.execution.Scheduler
import scala.util.Try
import loamstream.util.RunResults

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
        scheduler: Scheduler,
        isSuccess: RunResults.SuccessPredicate/*  = zeroIsSuccess */): CommandInvoker.Async[P] = {
      
      def invokeBinaryFor(param: P) = Try {
        val tokens = makeTokens(binaryName, param)
        
        debug(s"Invoking '$binaryName': '${tokens.mkString(" ")}'")
        
        Processes.runSync(tokens)()
      }
      
      val notRetrying = maxRetries == 0
      
      val invokeOnce = {
        implicit val sch = scheduler
        
        new CommandInvoker.Async.JustOnce[P](binaryName, invokeBinaryFor, isSuccess)
      }
      
      if(notRetrying) {
        invokeOnce
      } else {
        new CommandInvoker.Async.Retrying[P](delegate = invokeOnce, maxRetries = maxRetries, scheduler = scheduler)
      }
    }
  
    def makeTokens(actualBinary: String, param: P): Seq[String]
  }
}
