package loamstream.util

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import CommandInvoker.InvocationFn
import CommandInvoker.AsyncInvocationFn
import monix.reactive.Observable
import monix.execution.Scheduler
import scala.concurrent.duration.FiniteDuration

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
object CommandInvoker {
  type InvocationFn[A] = A => Try[RunResults]
  
  type SuccessfulInvocationFn[A] = A => Try[RunResults.Successful]

  type AsyncInvocationFn[A] = A => Future[RunResults.Successful]

  trait Sync[A] extends (SuccessfulInvocationFn[A])

  trait Async[A] extends (AsyncInvocationFn[A])
  
  object Sync {
    final class JustOnce[A](
      val binaryName: String,
      delegateFn: InvocationFn[A]) extends CommandInvoker.Sync[A] with Loggable {

      override def apply(param: A): Try[RunResults.Successful] = delegateFn(param).flatMap(_.tryAsSuccess)
    }

    final class Retrying[A](
      delegate: JustOnce[A],
      maxRetries: Int,
      delayStart: FiniteDuration = Retrying.defaultDelayStart,
      delayCap: FiniteDuration = Retrying.defaultDelayCap) extends CommandInvoker.Sync[A] with Loggable {

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
        delayStart: FiniteDuration,
        delayCap: FiniteDuration): A => Try[RunResults.Successful] = { param =>

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

    object Retrying {
      def apply[A](
        maxRetries: Int,
        binaryName: String,
        delegateFn: InvocationFn[A],
        delayStart: FiniteDuration = Retrying.defaultDelayStart,
        delayCap: FiniteDuration = Retrying.defaultDelayCap): Retrying[A] = {

        new Retrying(
          new JustOnce[A](binaryName, delegateFn),
          maxRetries,
          delayStart,
          delayCap)
      }

      import scala.concurrent.duration._

      val defaultDelayStart: FiniteDuration = 0.5.seconds
      val defaultDelayCap: FiniteDuration = 30.seconds
    }
  }

  object Async {
    final class JustOnce[A](
      val binaryName: String,
      delegateFn: InvocationFn[A])(implicit scheduler: Scheduler, logCtx: LogContext) extends CommandInvoker.Async[A] {

      override def apply(param: A): Future[RunResults.Successful] = {
        import Observables.Implicits._

        invokeBinary(param).firstAsFuture.flatMap(Future.fromTry)
      }

      private[CommandInvoker] def invokeBinary(param: A): Observable[Try[RunResults.Successful]] = Observable {
        delegateFn(param).flatMap(_.tryAsSuccess(logCtx))
      }
    }

    final class Retrying[A](
      delegate: JustOnce[A],
      maxRetries: Int,
      delayStart: FiniteDuration = Retrying.defaultDelayStart,
      delayCap: FiniteDuration = Retrying.defaultDelayCap,
      scheduler: Scheduler)(implicit logCtx: LogContext) extends CommandInvoker.Async[A] {

      override def apply(param: A): Future[RunResults.Successful] = runCommand(param)

      private val runCommand: AsyncInvocationFn[A] = {
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
        delayStart: FiniteDuration,
        delayCap: FiniteDuration,
        scheduler: Scheduler): AsyncInvocationFn[A] = { param =>

        val maxRuns = maxRetries + 1

        val resultOptObs = Loops.retryUntilSuccessWithBackoffAsync(maxRuns, delayStart, delayCap, scheduler) {
          delegate.invokeBinary(param)
        }

        import Observables.Implicits._

        implicit val sch = scheduler
        
        val result: Future[RunResults.Successful] = resultOptObs.firstAsFuture.flatMap {
          case Some(a) => Future.successful(a)
          case _ => {
            val msg = s"Invoking '$binaryName' with param '$param' failed after $maxRuns runs"

            logCtx.debug(msg)

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
        delayStart: FiniteDuration = Retrying.defaultDelayStart,
        delayCap: FiniteDuration = Retrying.defaultDelayCap,
        scheduler: Scheduler)(implicit logCtx: LogContext): Retrying[A] = {

        new Retrying(
          new JustOnce[A](binaryName, delegateFn)(scheduler, logCtx),
          maxRetries,
          delayStart,
          delayCap,
          scheduler)
      }

      import scala.concurrent.duration._

      val defaultDelayStart: FiniteDuration = 0.5.seconds
      val defaultDelayCap: FiniteDuration = 30.seconds
    }
  }
}
