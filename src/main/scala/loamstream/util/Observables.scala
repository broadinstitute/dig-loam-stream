package loamstream.util

import rx.lang.scala.Observable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import rx.lang.scala.Subject
import rx.lang.scala.subjects.ReplaySubject

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
   * @return a future map of keys to values
   */
  def toMap[A,B](tuples: Traversable[(A, Observable[B])]): Observable[Map[A,B]] = {
    val z: Observable[Map[A,B]] = Observable.just(Map.empty)
    
    tuples.foldLeft(z) { (observableAcc, tuple) =>
      val (a, obsB) = tuple
      
      for {
        acc <- observableAcc
        b <- obsB
      } yield {
        acc + (a -> b)
      }
    }
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
    import rx.lang.scala.JavaConversions.{toScalaObservable, toJavaObservable}
    import scala.collection.JavaConverters._

    //NB: Cast 'should be' safe.  It's needed because toJavaObservable, when given an rx.lang.scala.Observable[A],
    //returns an rx.Observable[_ <: A].  Combined with converting the scala.Iterable to a java.lang.Iterable, this
    //means we would have a java.lang.Iterable[rx.Observable[_ <: A]], which rx.Observable.merge won't accept. :(
    //The cast is safe since the Java Observable made from the Scala one is guaranteed to emit the same values.
    val javaObservables: Iterable[rx.Observable[A]] = os.map(toJavaObservable).map(_.asInstanceOf[rx.Observable[A]])
    
    val javaIterableOfJavaObservables: java.lang.Iterable[rx.Observable[A]] = javaObservables.asJava
    
    toScalaObservable(rx.Observable.merge(javaIterableOfJavaObservables))
  }
}