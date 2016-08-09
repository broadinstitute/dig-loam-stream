package loamstream.util

import monix.reactive.Observable
import monix.execution.Scheduler
import monix.reactive.OverflowStrategy
import monix.execution.Cancelable

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
     * with a new method in Monix proper.
     */
    def until(p: A => Boolean)(implicit scheduler: Scheduler): Observable[A] = {
      //TODO: Revisit Overflow Strategy
      Observable.create(OverflowStrategy.Unbounded) { downstream =>
        def complete() = downstream.onComplete()
        
        val cancelable = obs.foreach { a =>
          downstream.onNext(a)
          
          if(p(a)) {
            complete()
          } 
        }
        
        //TODO: There must be a better way.
        cancelable.foreach(_ => complete())
        
        cancelable
      }
    }
  }
}