package loamstream.util

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.scalatest.FunSuite

import rx.lang.scala.Observable

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

  test("firstAsFuture") {
    val f = Observable.just("a", "b", "c", "d").firstAsFuture

    assert(waitFor(f) == "a")
  }

  test("lastAsFuture") {
    val f = Observable.just("a", "b", "c", "d").lastAsFuture

    assert(waitFor(f) == "d")
  }
}