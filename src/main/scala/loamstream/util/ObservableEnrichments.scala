package loamstream.util

import monix.reactive.Observable
import monix.execution.Scheduler
import monix.reactive.OverflowStrategy
import monix.execution.Cancelable

/**
 * @author clint
 * date: Jul 1, 2016
 */
object ObservableEnrichments {
  final implicit class ObservableOps[A](val obs: Observable[A]) extends AnyVal {
    def takeUntil(p: A => Boolean)(implicit scheduler: Scheduler): Observable[A] = {
      //TODO: Revisit Overflow Strategy
      Observable.create(OverflowStrategy.Unbounded) { downstream =>
        val cancelable = obs.foreach { a =>
          downstream.onNext(a)
          
          if(p(a)) {
            downstream.onComplete()
          } 
        }
        
        cancelable.foreach(_ => downstream.onComplete())
        
        cancelable
      }
    }
  }
}