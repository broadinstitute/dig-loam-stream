package loamstream.util

import scala.concurrent.duration.DurationInt

import org.scalatest.FunSuite

import rx.lang.scala.Observable

/**
 * @author clint
 * date: Aug 26, 2016
 */
final class ObservablesTest extends FunSuite {
  
  import loamstream.TestHelpers.waitFor
  import ObservableEnrichments._
  import Observables.sequence
  
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
      
      val expected = Seq(Map("a" -> 1, "b" -> 2, "c" -> 3), Map("a" -> 1, "b" -> 22, "c" -> 3))
      
      assert(waitFor(toMap(tuples).to[Seq].firstAsFuture) == expected)
    }
  }
  
  private def doReduceMapsTest(os: Seq[Observable[Map[String,Int]]], expected: Map[String, Int]): Unit = {
    import Observables.reduceMaps
    
    waitFor(reduceMaps(os).to[Seq].firstAsFuture) === Seq(expected)
  }
  
  test("reduceMaps - empty input") {
    doReduceMapsTest(Seq.empty, Map.empty)
  }
    
  test("reduceMaps - empty inputs (plural)") {
    doReduceMapsTest(Seq(Observable.empty, Observable.empty, Observable.empty), Map.empty)
  }

  test("reduceMaps - non-empty input") {
    val m0 = Map("a" -> 1, "b"-> 2)
    val m1 = Map("c" -> 3)
    val m2 = Map("d" -> 4, "e"-> 5)
    val m3 = Map("f" -> 5)
    val m4 = Map("g" -> 6, "h"-> 7)
    
    val o0 = Observable.just(m0)
    val o1 = Observable.just(m1, m2)
    val o2 = Observable.just(m3, m4)
    
    val expected = Map(
        "a" -> 1, 
        "b"-> 2,
        "c" -> 3,
        "d" -> 4, 
        "e"-> 5,
        "f" -> 5,
        "g" -> 6, 
        "h"-> 7)
    
    doReduceMapsTest(Seq(o0, o1, o2), expected)
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
}
