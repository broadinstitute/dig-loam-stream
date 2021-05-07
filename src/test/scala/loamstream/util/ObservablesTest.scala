package loamstream.util

import scala.concurrent.Future
import scala.concurrent.duration._

import org.scalatest.FunSuite

import monix.reactive.Observable
import monix.reactive.subjects.PublishSubject
import monix.execution.Scheduler
import loamstream.TestHelpers
import monix.reactive.subjects.Subject
import monix.execution.Ack



/**
 * @author clint
 * date: Aug 26, 2016
 */
final class ObservablesTest extends FunSuite {
  
  import Observables.Implicits._
  import Observables.sequence
  import loamstream.TestHelpers.waitFor
  
  private def isEven(i: Int): Boolean = i % 2 == 0
  private def isOdd(i: Int): Boolean = !isEven(i)
  
  private def makeObservable[A](period: Int, msgs: A*): Observable[A] = {
    if(period == 0) {
      Observable.from(msgs)
    } else {
      Observable.interval(period.milliseconds).zip(Observable.fromIterable(msgs)).collect { case (_, msg) => msg }
    }
  }
  
  private implicit val scheduler = Scheduler.global
  
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
    
    val os: Seq[Observable[String]] = Seq(
        makeObservable(50, a, x), 
        makeObservable(200, b, y), 
        makeObservable(100, c, z))
    
    val actual = sequence(os).map(_.toSet).take(2).toListL.runSyncUnsafe(TestHelpers.defaultWaitTime)
    
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
    
    val actual = sequence(os).map(_.toSet).take(2).toListL.runSyncUnsafe(TestHelpers.defaultWaitTime)
    
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
    
    val actual = o.toListL.runSyncUnsafe(TestHelpers.defaultWaitTime)
    
    assert(actual === Seq(42))
  }
  
  test("observeAsync - exception thrown") {
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val o = Observables.observeAsync(throw new Exception("nuh"))
      
    intercept[Exception] {
      waitFor(o.firstAsFuture)
    }
  }
  
  test("toMap") {
    import Observables.toMap
    
    assert(waitFor(toMap(Nil).lastAsFuture) == Map.empty)
    
    {
      val tuples = Seq("a" -> Observable(1), "b" -> Observable(2), "c" -> Observable(3))
      
      assert(waitFor(toMap(tuples).lastAsFuture) == Map("a" -> 1, "b" -> 2, "c" -> 3))
    }
    
    {
      val tuples = Seq("a" -> Observable(1), "b" -> Observable(2, 22), "c" -> Observable(3))

      val expected = Seq(Map("a" -> 1, "b" -> 22, "c" -> 3))
      
      assert(toMap(tuples).toListL.runSyncUnsafe(TestHelpers.defaultWaitTime) == expected)
    }
  }
  
  test("merge - empty input") {
    import Observables.merge
    
    val shouldBeEmpty = Observables.merge[Int](Nil)
    
    assert(waitFor(shouldBeEmpty.isEmpty.firstAsFuture))
  }
  
  test("merge - empty inputs (plural)") {
    import Observables.merge
    
    val os: Seq[Observable[Int]] = Seq(Observable.empty, Observable.empty, Observable.empty)  
    
    val shouldBeEmpty = Observables.merge(os)
    
    assert(waitFor(shouldBeEmpty.isEmpty.firstAsFuture))
  }
  
  test("merge - non-empty") {
    import Observables.merge
    
    /* 
     * os is roughly Seq(
     *   Observable.just(a),
     *   Observable.just(aa,bb),
     *   Observable.just(aaa,bbb,ccc),
     *   Observable.just(aaaa,bbbb,cccc,dddd),
     *   Observable.just(aaaaa,bbbbb,ccccc,ddddd))
     */
    val os: Seq[Observable[String]] = for {
      i <- (1 to 5)
      strs = ('a' to 'e').take(i).map(_.toString * i)
    } yield {
      Observable.from(strs)
    }
    
    val strs = merge(os).toListL.runSyncUnsafe(TestHelpers.defaultWaitTime)
    
    val expected = Seq(
        "a", 
        "aa",
        "aaa",
        "aaaa",
        "aaaaa",
        "bb", 
        "bbb", 
        "bbbb",
        "bbbbb",
        "ccc", 
        "cccc", 
        "ccccc",
        "dddd", 
        "ddddd", 
        "eeeee")
    
    assert(strs.sorted === expected)
  }
  
  test("until") {
    def observable = Observable(2, 4, 6, 7, 8, 9, 10) 

    def toSeq[A](o: Observable[A]): Seq[A] = o.toListL.runSyncUnsafe(TestHelpers.defaultWaitTime)
    
    {
      val is = toSeq(observable.until(isOdd))

      assert(is == Seq(2, 4, 6, 7)) 
    }

    {
      val is = toSeq(observable.until(isEven))

      assert(is == Seq(2))
    }

    {
      val is = toSeq(observable.until(_ == 8))

      assert(is == Seq(2, 4, 6, 7, 8)) 
    }

    {
      val is = toSeq(observable.until(_ == 42))

      assert(is == Seq(2, 4, 6, 7, 8, 9, 10)) 
    }
  }
  
  test("until - errors are propagated properly") {
    val e1 = new Exception("blarg")
    val e2 = new Exception("nuh")
    
    assert(e1 !== e2)
    
    def doTest(toThrow: Throwable, expected: Option[Throwable]): Unit = { 
      val source: Subject[Int, Int] = PublishSubject[Int]()
      
      val limited = source.until(_ == 42)
      
      val current: ValueBox[Int] = ValueBox(0)
      val currentException: ValueBox[Option[Throwable]] = ValueBox(None)
      
      def onNext(i: Int): Future[Ack] = {
        current := i
        
        Ack.Continue
      }
      
      def onError(t: Throwable): Unit = {
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
      
      val source: Subject[Int, Int] = PublishSubject[Int]()
        
      val limited = source.until(_ == 42)
      
      def noop[A]: A => Unit = _ => ()
      
      def onNext[A]: A => Future[Ack] = _ => Ack.Continue
      
      limited.subscribe(onNext, noop, () => completed := true)
      
      assert(completed() === false)
      
      source.onNext(1)
      
      assert(completed() === false)
      
      if(completeEarly) {
        source.onComplete()
      } else {
        source.onNext(42)
      }
      
      assert(completed() === true)
    }
    
    doTest(completeEarly = true)
    doTest(completeEarly = false)
  }

  test("firstAsFuture") {
    val f = Observable("a", "b", "c", "d").firstAsFuture

    assert(waitFor(f) == "a")
  }

  test("lastAsFuture") {
    val f = Observable("a", "b", "c", "d").lastAsFuture

    assert(waitFor(f) == "d")
  }
}
