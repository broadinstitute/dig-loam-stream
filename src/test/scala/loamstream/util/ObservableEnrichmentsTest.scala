package loamstream.util

import org.scalatest.FunSuite
import monix.reactive.Observable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.Future

/**
 * @author clint
 * date: Jul 1, 2016
 */
final class ObservableEnrichmentsTest extends FunSuite {
  private def isEven(i: Int): Boolean = i % 2 == 0
  private def isOdd(i: Int): Boolean = !isEven(i)
  
  test("until") {
    val observable = Observable.fromIterable(Seq(2,4,6,7,8,9,10)) // scalastyle:ignore magic.number
    
    import monix.execution.Scheduler.Implicits.global
    
    import ObservableEnrichments._
    
    def waitFor[A](f: Future[A]): A = Await.result(f, Duration.Inf)
    
    {
      val buf = new ArrayBuffer[Int]
    
      val fut = observable.until(isOdd).foreach(buf += _)
    
      waitFor(fut)
    
      assert(buf == Seq(2,4,6,7)) // scalastyle:ignore magic.number
    }
    
    {
      val buf = new ArrayBuffer[Int]
    
      val fut = observable.until(isEven).foreach(buf += _)
    
      waitFor(fut)
    
      assert(buf == Seq(2))
    }
    
    {
      val buf = new ArrayBuffer[Int]
    
      val fut = observable.until(_ == 8).foreach(buf += _)
    
      waitFor(fut)
    
      assert(buf == Seq(2,4,6,7,8)) // scalastyle:ignore magic.number
    }
    
    {
      val buf = new ArrayBuffer[Int]
    
      val fut = observable.until(_ == 42).foreach(buf += _)
    
      waitFor(fut)
    
      assert(buf == Seq(2,4,6,7,8,9,10)) // scalastyle:ignore magic.number
    }
  }
}