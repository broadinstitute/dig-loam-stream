package loamstream.util

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.scalatest.FunSuite

import rx.lang.scala.Observable
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.Subject

/**
 * @author clint
 * date: Jul 1, 2016
 */
final class ObservableEnrichmentsTest extends FunSuite {
  private def isEven(i: Int): Boolean = i % 2 == 0
  private def isOdd(i: Int): Boolean = !isEven(i)
  
  private def waitFor[A](f: Future[A]): A = Await.result(f, Duration.Inf)

  import ObservableEnrichments._
  
  test("until") {
    def observable = Observable.just(2, 4, 6, 7, 8, 9, 10) // scalastyle:ignore magic.number

    def toFuture[A](o: Observable[A]): Future[Seq[A]] = o.to[Seq].firstAsFuture
    
    {
      val fut = toFuture(observable.until(isOdd))

      assert(waitFor(fut) == Seq(2, 4, 6, 7)) // scalastyle:ignore magic.number
    }

    {
      val fut = toFuture(observable.until(isEven))

      assert(waitFor(fut) == Seq(2))
    }

    {
      val fut = toFuture(observable.until(_ == 8))

      assert(waitFor(fut) == Seq(2, 4, 6, 7, 8)) // scalastyle:ignore magic.number
    }

    {
      val fut = toFuture(observable.until(_ == 42))

      assert(waitFor(fut) == Seq(2, 4, 6, 7, 8, 9, 10)) // scalastyle:ignore magic.number
    }
  }
  
  test("until - errors are propagated properly") {
    val e1 = new Exception("blarg")
    val e2 = new Exception("nuh")
    
    assert(e1 !== e2)
    
    def doTest(toThrow: Throwable, expected: Option[Throwable]): Unit = { 
      val source: Subject[Int] = PublishSubject[Int]()
      
      val limited = source.until(_ == 42)
      
      val current: ValueBox[Int] = ValueBox(0)
      val currentException: ValueBox[Option[Throwable]] = ValueBox(None)
      
      def onNext(i: Int): Unit = current := i
      
      def onError(t: Throwable): Unit = {
        println(s"onError: $t")
        
        if(t.getMessage == "blarg") { currentException := Some(t) } else { currentException := None }
      }
      
      limited.subscribe(onNext, onError)
      
      assert(current() === 0)
      assert(currentException() === None)
      
      source.onNext(1)
      
      assert(current() === 1)
      assert(currentException() === None)
      
      source.onError(toThrow)

      assert(current() === 1)
      assert(currentException() === expected)
    }
    
    doTest(e1, Some(e1))
    doTest(e2, None)
  }
  
  test("until - completion is propagated properly") {
    def doTest(completeEarly: Boolean = true): Unit = {
      val completed: ValueBox[Boolean] = ValueBox(false)
      
      val source: Subject[Int] = PublishSubject[Int]()
        
      val limited = source.until(_ == 42)
      
      def noop[A]: A => Unit = _ => ()
      
      limited.subscribe(noop, noop, () => completed := true)
      
      assert(completed() === false)
      
      source.onNext(1)
      
      assert(completed() === false)
      
      if(completeEarly) {
        source.onCompleted()
      } else {
        source.onNext(42)
      }
      
      assert(completed() === true)
    }
    
    doTest(completeEarly = true)
    doTest(completeEarly = false)
  }

  test("firstAsFuture") {
    val f = Observable.just("a", "b", "c", "d").firstAsFuture

    assert(waitFor(f) == "a")
  }

  test("lastAsFuture") {
    val f = Observable.just("a", "b", "c", "d").lastAsFuture

    assert(waitFor(f) == "d")
  }
}
