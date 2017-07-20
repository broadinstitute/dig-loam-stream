package loamstream.util

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success

import rx.lang.scala.Observable

/**
 * @author clint
 * date: Jul 1, 2016
 *
 * Extension methods for Monix Observables.  Adds combinators that make some LoamStream use cases easier.
 */
object ObservableEnrichments {
  final implicit class ObservableOps[A](val obs: Observable[A]) extends AnyVal {
    /**
     * Returns an Observable that emits elements that satisfy the passed-in predicate, up to and including the FIRST
     * element that does not satisy the predicate.  The returned observable completes after emitting the first element
     * that satisfies the predicate.
     *
     * For example, for a well-behaved Uger job, say we have
     *
     * val statuses: Obervable[JobStatus] = ... // monitor some job
     *
     * statuses.until(_.isFinished)
     *
     * will produce an Observable that will emit elements like
     *
     * JobStatus.Queued, JobStatus.Running, JobStatus.Running, ... , JobStatus.Done
     *
     * Or suppose we had
     *
     * val numbers = Observable(1,2,3,4,5,6,7,8,9,10) //an Observable that emits the elements passed to apply()
     *
     * numbers.until(_ >= 6)
     *
     * Would produce an Observable that yields
     *
     * 1,2,3,4,5,6
     *
     * Note that this is different from the built-in .takeWhile, which returns elements that satisfy a predicate, BUT
     * NOT any that don't.
     *
     * numbers.takeWhile(_ < 6)
     *
     * would return an Observable that yields
     *
     * 1,2,3,4,5
     *
     * or for the job example,
     *
     * statuses.takeWhile(_.notFinished)
     *
     * would return an Observable that might yield
     *
     * JobStatus.Queued, JobStatus.Running, ..., JobStatus.Running
     *
     * (Note the missing 'terminal' status.)
     *
     * @param p the predicate to use to decide what events to emit
     * @return an Observable that emits elements that satisfy the passed-in predicate, up to and including the FIRST
     * element that does not satisy the predicate.
     *
     * NB: Note that this was renamed to 'until' from 'takeUntil'.  The latter would be preferrable, but it conflicts
     * with a method in RxScala proper.
     */
    def until(p: A => Boolean): Observable[A] = {
      Observable { subscriber =>
        def onNext(a: A): Unit = {
          subscriber.onNext(a)

          if (p(a)) {
            subscriber.onCompleted()
          }
        }
        
        obs.foreach(onNext, subscriber.onError, () => subscriber.onCompleted())
      }
    }
    
    /**
     * Returns a Future that will contain the FIRST value fired from the wrapped Observable.
     *
     * If the wrapped Observable is empty, the returned Future will be completed with a Failure.
     */
    def firstAsFuture: Future[A] = asFuture(obs.headOption)

    /**
     * Returns a Future that will contain the LAST value fired from the wrapped Observable.
     *
     * Note that the returned Future will ONLY complete if the wrapped Observable does.
     *
     * If the wrapped Observable is empty, the returned Future will be completed with a Failure.
     */
    def lastAsFuture: Future[A] = asFuture(obs.lastOption)

    private def asFuture(o: Observable[Option[A]]): Future[A] = {
      val p: Promise[A] = Promise()

      def completeDueToNoValue(): Unit = p.complete(Tries.failure("Observable emitted no values"))

      def onNext(o: Option[A]): Unit = o match {
        case Some(a) => p.complete(Success(a))
        case None    => completeDueToNoValue()
      }

      def onError(e: Throwable): Unit = p.complete(Failure(e))

      o.first.foreach(onNext, onError)

      p.future
    }
  }
}
