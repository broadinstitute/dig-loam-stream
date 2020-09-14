package loamstream.util

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success

import rx.lang.scala.Observable
import scala.concurrent.duration.Duration

/**
 * @author clint
 * date: Aug 26, 2016
 * 
 * An object to hold utility methods operating on Observables
 */
object Observables extends Loggable {
  /**
   * Turn a Seq of Observables into an Observable that produces Seqs, a la Future.sequence.
   * 
   * If the input Seq is empty, return an Observable that will immediately fire Nil to all subscribers.
   * Otherwise, return the result of Observable.zip(os)
   * 
   * @param os: the sequence of observables to transform
   * @see Observable.zip 
   * 
   * Note that this method takes a Seq and returns an Observable[Seq[A]].  Making the return type depend
   * on the param type, like Future.sequence, is possible but inconvenient due to the signature of 
   * Observable.zip(), the method this one delegates most of its work to.
   */
  def sequence[A](os: Seq[Observable[A]]): Observable[Seq[A]] = {
    if (os.isEmpty) { Observable.just(Nil) }
    else {
      Observable.zip(Observable.from(os))
    }
  }
  
  /**
   * Runs a chunk of code asynchronously via the supplied ExecutionContext, and returns an
   * Observable that will fire (and then complete) when the code chunk finishes running.
   * 
   * @param a the chunk of code to run
   */
  def observeAsync[A](a: => A)(implicit context: ExecutionContext): Observable[A] = {
    Observable.from(Futures.runBlocking(a))
  }
  
  /**
   * Folds a bunch of keys and observables producing values into an observable producing maps of keys to values.
   * 
   * @param tuples a collection of 2-tuples containing a key of type A, and an observable producing values of type B.
   * @param context the ExecutionContext to run on
   * @return an Observable producing map of keys to values
   */
  def toMap[A,B](tuples: Traversable[(A, Observable[B])]): Observable[Map[A,B]] = {
    //NB: Use merge() instead of folding over the `tuples` Traversable to avoid blowing the stack.
    
    val tupleObservables: Iterable[Observable[(A, B)]] = tuples.toIterable.map { case (a, bs) => bs.map(b => (a, b)) }
    
    val tupleObs: Observable[(A, B)] = merge(tupleObservables)
    
    tupleObs.foldLeft(Map.empty[A,B])(_ + _)
  }
  
  /**
   * Expose the method rx.Observables.merge in a Scala-friendly way.  Given
   * 
   * val os: Iterable[Observable[A]] = ...
   * 
   * this is an alternative way to turn os into an Observable[A] that avoids blowing the stack, as something like
   * 
   * os.reduce(_ merge _) 
   * 
   * will, given enough Observables.
   * 
   * @param os A collection of Observables to merge.
   * @return An Observable that emits the values emitted by the input Observables, interleaved.
   * @see http://reactivex.io/documentation/operators/merge.html
   * @see http://reactivex.io/RxJava/javadoc/rx/Observable.html#merge(java.lang.Iterable)
   */
  def merge[A](os: Iterable[Observable[A]]): Observable[A] = {
    import rx.lang.scala.JavaConversions.{ toJavaObservable, toScalaObservable }
    import scala.collection.JavaConverters._

    //NB: Cast 'should be' safe.  It's needed because toJavaObservable, when given an rx.lang.scala.Observable[A],
    //returns an rx.Observable[_ <: A].  Combined with converting the scala.Iterable to a java.lang.Iterable, this
    //means we would have a java.lang.Iterable[rx.Observable[_ <: A]], which rx.Observable.merge won't accept. :(
    //The cast is safe since the Java Observable made from the Scala one is guaranteed to emit the same values.
    val javaObservables: Iterable[rx.Observable[A]] = os.map(toJavaObservable).map(_.asInstanceOf[rx.Observable[A]])
    
    val javaIterableOfJavaObservables: java.lang.Iterable[rx.Observable[A]] = javaObservables.asJava
    
    toScalaObservable(rx.Observable.merge(javaIterableOfJavaObservables))
  }
  
  def merge[A](first: Observable[A], rest: Observable[A]*): Observable[A] = merge(first +: rest)
  
  /**
   * @author clint
   * date: Jul 1, 2016
   *
   * Extension methods for Rx Observables.  Adds combinators that make some LoamStream use cases easier.
   */
  object Implicits {
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
  
        o.firstOrElse(None).foreach(onNext, onError)
  
        p.future
      }
    }
  }
}
