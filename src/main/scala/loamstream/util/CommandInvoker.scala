package loamstream.util

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import CommandInvoker.InvocationFn
import CommandInvoker.SuccessfulInvocationFn
import rx.lang.scala.Scheduler
import rx.lang.scala.Observable

/**
 * @author clint
 * Apr 25, 2019
 * 
 * Runs a function, delegateFn, up to maxRetries + 1 times, and returns the first Success, or a Failure otherwise.
 * 
 * Used to run command-line utils synchronously, retrying them if their exit codes are non-zero.  Waits greater
 * and greater periods after each failure, starting at delayStart and doubling after each failure, up to a max of
 * delayCap.  See Loops.Backoff.delaySequence and Loops.retryUntilSuccessWithBackoff . 
 */
trait CommandInvoker[A] extends (SuccessfulInvocationFn[A])

object CommandInvoker {
  type InvocationFn[A] = A => Try[RunResults]
  
  type SuccessfulInvocationFn[A] = A => Future[RunResults.Successful]
  
  trait Sync[A] extends (InvocationFn[A])
  
  final class JustOnce[A](
      val binaryName: String,
      delegateFn: InvocationFn[A])(implicit ec: ExecutionContext) extends CommandInvoker[A] with Loggable {
    
    override def apply(param: A): Future[RunResults.Successful] = {
      import Observables.Implicits._
      
      invokeBinary(param).firstAsFuture.flatMap(Future.fromTry)
    }
    
    private[CommandInvoker] def invokeBinary(param: A): Observable[Try[RunResults.Successful]] = Observable.just {
      delegateFn(param) match {
        //Coerce invocations producing non-zero exit codes to Failures
        case Success(r: RunResults.Unsuccessful) => {
          val msg = s"Error invoking '${r.commandLine}' (exit code ${r.exitCode})"
          
          r.logStdOutAndStdErr(s"$msg; output streams follow:", Loggable.Level.Warn)
          
          Tries.failure(msg)
        }
        case Success(r: RunResults.Successful) => Success(r)
        //pass failure-failures and successful (0 exit code) invocations through
        case Failure(e) => Failure(e)
      }
    }
  }
  
  final class SyncJustOnce[A](
      val binaryName: String,
      delegateFn: InvocationFn[A]) extends CommandInvoker.Sync[A] with Loggable {
    
    override def apply(param: A): Try[RunResults.Successful] = delegateFn(param) match {
      //Coerce invocations producing non-zero exit codes to Failures
      case Success(r: RunResults.Unsuccessful) => {
        val msg = s"Error invoking '${r.commandLine}' (exit code ${r.exitCode})"
        
        r.logStdOutAndStdErr(s"$msg; output streams follow:", Loggable.Level.Warn)
        
        Tries.failure(msg)
      }
      case Success(r: RunResults.Successful) => Success(r)
      //pass failure-failures and successful (0 exit code) invocations through
      case Failure(e) => Failure(e)
    }
  }
  
  final class Retrying[A](
      delegate: JustOnce[A],
      maxRetries: Int,
      delayStart: Duration = Retrying.defaultDelayStart,
      delayCap: Duration = Retrying.defaultDelayCap,
      scheduler: Scheduler)(implicit ec: ExecutionContext) extends CommandInvoker[A] with Loggable {
  
    override def apply(param: A): Future[RunResults.Successful] = runCommand(param)
  
    private val runCommand: SuccessfulInvocationFn[A] = {
      doRetries(
          binaryName = delegate.binaryName, 
          maxRetries = maxRetries, 
          delayStart = delayStart, 
          delayCap = delayCap, 
          scheduler = scheduler)
    }
    
    private def doRetries(
      binaryName: String,
      maxRetries: Int,
      delayStart: Duration,
      delayCap: Duration,
      scheduler: Scheduler): SuccessfulInvocationFn[A] = { param =>
        
      val maxRuns = maxRetries + 1
      
      val resultOptObs = Loops.retryUntilSuccessWithBackoffAsync(maxRuns, delayStart, delayCap, scheduler) {
        delegate.invokeBinary(param)
      }
      
      import Observables.Implicits._
      
      val result: Future[RunResults.Successful] = resultOptObs.firstAsFuture.flatMap {
        case Some(a) => Future.successful(a)
        case _ => {
          val msg = s"Invoking '$binaryName' with param '$param' failed after $maxRuns runs"
          
          debug(msg)
  
          Future.failed(new Exception(msg))
        }
      }
      
      result
    }
  }
  
  object Retrying {
    def apply[A](
      maxRetries: Int,
      binaryName: String,
      delegateFn: InvocationFn[A],
      delayStart: Duration = Retrying.defaultDelayStart,
      delayCap: Duration = Retrying.defaultDelayCap,
      scheduler: Scheduler)(implicit ec: ExecutionContext): Retrying[A] = {

      new Retrying(
          new JustOnce[A](binaryName, delegateFn),
          maxRetries,
          delayStart,
          delayCap,
          scheduler)
    }
    
    import scala.concurrent.duration._
  
    val defaultDelayStart: Duration = 0.5.seconds
    val defaultDelayCap: Duration = 30.seconds
  }
  
  final class SyncRetrying[A](
      delegate: SyncJustOnce[A],
      maxRetries: Int,
      delayStart: Duration = Retrying.defaultDelayStart,
      delayCap: Duration = Retrying.defaultDelayCap) extends CommandInvoker.Sync[A] with Loggable {
  
    override def apply(param: A): Try[RunResults.Successful] = {
      doRetries(
          binaryName = delegate.binaryName, 
          maxRetries = maxRetries, 
          delayStart = delayStart, 
          delayCap = delayCap)(param)
    }
  
    private def doRetries(
      binaryName: String,
      maxRetries: Int,
      delayStart: Duration,
      delayCap: Duration): A => Try[RunResults.Successful] = { param =>
        
      val maxRuns = maxRetries + 1
      
      val resultOpt = Loops.retryUntilSuccessWithBackoff(maxRuns, delayStart, delayCap) {
        delegate(param)
      }
      
      import Observables.Implicits._
      
      resultOpt match {
        case Some(a) => Success(a)
        case _ => {
          val msg = s"Invoking '$binaryName' with param '$param' failed after $maxRuns runs"
          
          debug(msg)
  
          Tries.failure(msg)
        }
      }
    }
  }
  
  object SyncRetrying {
    def apply[A](
      maxRetries: Int,
      binaryName: String,
      delegateFn: InvocationFn[A],
      delayStart: Duration = Retrying.defaultDelayStart,
      delayCap: Duration = Retrying.defaultDelayCap): SyncRetrying[A] = {

      new SyncRetrying(
          new SyncJustOnce[A](binaryName, delegateFn),
          maxRetries,
          delayStart,
          delayCap)
    }
    
    import scala.concurrent.duration._
  
    val defaultDelayStart: Duration = 0.5.seconds
    val defaultDelayCap: Duration = 30.seconds
  }
}
