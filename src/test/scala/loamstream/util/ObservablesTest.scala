package loamstream.util

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import org.scalatest.FunSuite

import rx.lang.scala.Observable
import rx.lang.scala.Subject
import rx.lang.scala.subjects.PublishSubject


/**
 * @author clint
 * date: Aug 26, 2016
 */
final class ObservablesTest extends FunSuite {
  
  import loamstream.TestHelpers.waitFor
  import Observables.Implicits._
  import Observables.sequence
  
  private def isEven(i: Int): Boolean = i % 2 == 0
  private def isOdd(i: Int): Boolean = !isEven(i)
  
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
    
    val os: Seq[Observable[String]] = Seq(
        makeObservable(50, a, x), 
        makeObservable(200, b, y), 
        makeObservable(100, c, z))
    
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
  
  test("toMap") {
    import Observables.toMap
    
    assert(waitFor(toMap(Nil).lastAsFuture) == Map.empty)
    
    {
      val tuples = Seq("a" -> Observable.just(1), "b" -> Observable.just(2), "c" -> Observable.just(3))
      
      assert(waitFor(toMap(tuples).lastAsFuture) == Map("a" -> 1, "b" -> 2, "c" -> 3))
    }
    
    {
      val tuples = Seq("a" -> Observable.just(1), "b" -> Observable.just(2, 22), "c" -> Observable.just(3))

      val expected = Seq(Map("a" -> 1, "b" -> 22, "c" -> 3))
      
      assert(waitFor(toMap(tuples).to[Seq].firstAsFuture) == expected)
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
    
    val strs = waitFor(merge(os).to[Seq].firstAsFuture)
    
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
    def observable = Observable.just(2, 4, 6, 7, 8, 9, 10) 

    def toFuture[A](o: Observable[A]): Future[Seq[A]] = o.to[Seq].firstAsFuture
    
    {
      val fut = toFuture(observable.until(isOdd))

      assert(waitFor(fut) == Seq(2, 4, 6, 7)) 
    }

    {
      val fut = toFuture(observable.until(isEven))

      assert(waitFor(fut) == Seq(2))
    }

    {
      val fut = toFuture(observable.until(_ == 8))

      assert(waitFor(fut) == Seq(2, 4, 6, 7, 8)) 
    }

    {
      val fut = toFuture(observable.until(_ == 42))

      assert(waitFor(fut) == Seq(2, 4, 6, 7, 8, 9, 10)) 
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
