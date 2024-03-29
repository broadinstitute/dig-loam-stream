package loamstream.util

import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import CommandInvoker.InvocationFn
import CommandInvoker.AsyncInvocationFn
import monix.reactive.Observable
import monix.execution.Scheduler
import scala.concurrent.duration.FiniteDuration
import monix.eval.Task

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
  type SuccessfulInvocationFn[A] = A => Try[RunResults.Completed]

  type AsyncInvocationFn[A] = A => Task[RunResults]
  type SuccessfulAsyncInvocationFn[A] = A => Task[RunResults.Completed]

  trait Sync[A] extends (SuccessfulInvocationFn[A])

  trait Async[A] extends (AsyncInvocationFn[A])
  
  import RunResults.SuccessPredicate.ByExitCode.zeroIsSuccess

  object Sync {
    final class JustOnce[A](
      val binaryName: String,
      delegateFn: InvocationFn[A],
      isSuccess: RunResults.SuccessPredicate) extends CommandInvoker.Sync[A] with Loggable {

      override def apply(param: A): Try[RunResults.Completed] = {
        delegateFn(param).flatMap(_.tryAsSuccess("", isSuccess))
      }
    }

    final class Retrying[A](
      delegate: JustOnce[A],
      maxRetries: Int,
      delayStart: FiniteDuration = Retrying.defaultDelayStart,
      delayCap: FiniteDuration = Retrying.defaultDelayCap) extends CommandInvoker.Sync[A] with Loggable {

      override def apply(param: A): Try[RunResults.Completed] = {
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
        delayCap: FiniteDuration): A => Try[RunResults.Completed] = { param =>

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
        delayCap: FiniteDuration = Retrying.defaultDelayCap,
        isSuccess: RunResults.SuccessPredicate/*  = zeroIsSuccess */): Retrying[A] = {

        new Retrying(
          new JustOnce[A](binaryName, delegateFn, isSuccess = isSuccess),
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
      delegateFn: InvocationFn[A],
      isSuccess: RunResults.SuccessPredicate/*  = zeroIsSuccess */)
     (implicit scheduler: Scheduler, logCtx: LogContext) extends CommandInvoker.Async[A] {

      override def apply(param: A): Task[RunResults.Completed] = {
        import Observables.Implicits._

        invokeBinary(param).firstL.flatMap(Task.fromTry)
      }

      private[CommandInvoker] def invokeBinary(param: A): Observable[Try[RunResults.Completed]] = Observable {
        delegateFn(param).flatMap(_.tryAsSuccess("", isSuccess)(logCtx))
      }
    }

    final class Retrying[A](
      delegate: JustOnce[A],
      maxRetries: Int,
      delayStart: FiniteDuration = Retrying.defaultDelayStart,
      delayCap: FiniteDuration = Retrying.defaultDelayCap,
      scheduler: Scheduler)(implicit logCtx: LogContext) extends CommandInvoker.Async[A] {

      override def apply(param: A): Task[RunResults.Completed] = runCommand(param)

      private val runCommand: SuccessfulAsyncInvocationFn[A] = {
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
        scheduler: Scheduler): SuccessfulAsyncInvocationFn[A] = { param =>

        val maxRuns = maxRetries + 1

        val resultOptObs = Loops.retryUntilSuccessWithBackoffAsync(maxRuns, delayStart, delayCap, scheduler) {
          delegate.invokeBinary(param)
        }

        import Observables.Implicits._
        //TODO: revisit use of global scheduler
        import Scheduler.Implicits.global
        
        val result: Task[RunResults.Completed] = resultOptObs.firstL.flatMap {
          case Some(a) => Task(a)
          case _ => {
            val msg = s"Invoking '$binaryName' with param '$param' failed after $maxRuns runs"

            logCtx.debug(msg)

            Task.raiseError(new Exception(msg))
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
        scheduler: Scheduler,
        isSuccess: RunResults.SuccessPredicate/*  = zeroIsSuccess */)(implicit logCtx: LogContext): Retrying[A] = {

        new Retrying(
          new JustOnce[A](binaryName, delegateFn, isSuccess)(scheduler, logCtx),
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
