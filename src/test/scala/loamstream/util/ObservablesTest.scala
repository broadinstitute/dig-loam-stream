package loamstream.util

import org.scalatest.FunSuite
import rx.lang.scala.Observable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * @author clint
 * date: Aug 26, 2016
 */
final class ObservablesTest extends FunSuite {
  import Observables.sequence
  import ObservableEnrichments._
  import Futures.waitFor
  
  private def makeObservable[A](period: Int, msgs: A*): Observable[A] = {
    if(period == 0) {
      Observable.from(msgs)
    } else {
      import scala.concurrent.duration._
      
      Observable.interval(period.milliseconds).zip(msgs).collect { case (_, msg) => msg }
    }
  }
  
  test("sequence") {
    val a = "A"
    val b = "B"
    val c = "C"
    
    val os: Seq[Observable[String]] = Seq(makeObservable(50, a), makeObservable(200, b), makeObservable(100, c))
    
    val future = sequence(os).take(1).lastAsFuture
    
    val actual = waitFor(future)
    
    //Use a set to ignore order
    assert(actual.toSet == Set(a, b, c))
  }
  
  test("sequence - multiple events") {
    val a = "A"
    val b = "B"
    val c = "C"
    
    val x = "X"
    val y = "Y"
    val z = "Z"
    
    val os: Seq[Observable[String]] = Seq(makeObservable(50, a, x), makeObservable(200, b, y), makeObservable(100, c, z))
    
    val future = sequence(os).map(_.toSet).take(2).to[Seq].lastAsFuture
    
    val actual = waitFor(future)
    
    //Use sets to ignore order
    val expected = Seq(Set(a,b,c), Set(x,y,z))
    
    assert(actual == expected)
  }
  
  test("sequence - multiple events, no delays") {
    val a = "A"
    val b = "B"
    val c = "C"
    
    val x = "X"
    val y = "Y"
    val z = "Z"
    
    val os: Seq[Observable[String]] = Seq(makeObservable(0, a, x), makeObservable(0, b, y), makeObservable(0, c, z))
    
    val future = sequence(os).map(_.toSet).take(2).to[Seq].lastAsFuture
    
    val actual = waitFor(future)
    
    //Use sets to ignore order
    val expected = Seq(Set(a,b,c), Set(x,y,z))
    
    assert(actual == expected)
  }

  test("sequence (empty input)") {
    
    val os: Seq[Observable[String]] = Nil
    
    val future = sequence(os).take(1).lastAsFuture
    
    val actual = waitFor(future)
    
    assert(actual == Nil)
  }
  
  test("observeAsync") {
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val o = Observables.observeAsync(42)
    
    val future = o.to[Seq].firstAsFuture
    
    assert(waitFor(future) == Seq(42))
  }
  
  test("observeAsync - exception thrown") {
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val o = Observables.observeAsync(throw new Exception("nuh"))
      
    intercept[Exception] {
      waitFor(o.firstAsFuture)
    }
  }
}