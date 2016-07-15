package loamstream.util

import org.scalatest.FunSuite

import scala.util.{Failure, Success, Try}

/**
  * @author clint
  *         date: Apr 29, 2016
  */
final class ShotTest extends FunSuite {
  private val toStr: Int => String = _.toString
  private val toInt: String => Int = _.toInt
  private val inc: Int => Int = _ + 1

  //scalastyle:off magic.number

  private val hit = Hit(42)

  private val snag = Snag("foo")

  private val miss = Miss(snag)

  private val incAndToString: Int => Shot[String] = i => Hit((i + 1).toString)

  test("apply()") {
    assert(Shot(42) === Hit(42))

    val e = new Exception with scala.util.control.NoStackTrace

    val shot = Shot {
      throw e
    }

    assert(shot === Miss(SnagThrowable(e)))
  }

  test("fromTry") {
    assert(Shot.fromTry(Success(42)) === Hit(42))

    val e = new Exception with scala.util.control.NoStackTrace

    val failure: Try[Int] = Failure(e)

    assert(Shot.fromTry(failure) === Miss(SnagThrowable(e)))
  }

  test("fromOption") {
    assert(Shot.fromOption(Some(42), Snag("This should never happen.")) === Hit(42))

    assert(Shot.fromOption(None, snag) === Miss(snag))
  }

  test("get") {
    assert(hit.get === 42)

    intercept[NoSuchElementException] {
      miss.get
    }
  }

  test("map") {
    assert(hit.map(identity) === hit)

    assert(hit.map(toStr) === Hit("42"))

    assert(hit.map(toStr).map(toInt).map(inc) === Hit(43))

    assert(hit.map(toStr andThen toInt andThen inc) === Hit(43))

    assert(miss.map(identity) === miss)

    assert(miss.map(toStr) === miss)

    assert(miss.map(toStr).map(toInt).map(inc) === miss)

    assert(miss.map(toStr andThen toInt andThen inc) === miss)
  }
  
  test("map (exception thrown)") {
    val e = new Exception
    
    val Miss(SnagThrowable(ex)) = hit.map(_ => throw e)
    
    assert(ex == e)
  }

  test("flatMap") {
    assert(hit.flatMap(_ => miss) === miss)
    assert(miss.flatMap(_ => miss) === miss)

    assert(hit.flatMap(incAndToString) === Hit("43"))
    assert(miss.flatMap(incAndToString) === miss)
  }
  
  test("flatMap (exception thrown)") {
    val e = new Exception
    
    val f: Int => Shot[Int] = _ => throw e
    
    val Miss(SnagThrowable(ex)) = hit.flatMap(f)
    
    assert(ex == e)
    
    assert(miss.flatMap(f) == miss)
  }
  
  test("flatten") {
    assert(Hit(miss).flatten == miss)
    assert(Hit(hit).flatten == hit)
    
    assert(Hit(Hit(Hit(Hit(42)))).flatten == Hit(Hit(Hit(42))))
    
    assert(Hit(Hit(Hit(Hit(miss)))).flatten == Hit(Hit(Hit(miss))))
  }

  test("Monad laws") {
    //Left Identity
    assert(incAndToString(42) === hit.flatMap(incAndToString))

    //Right identity
    assert(hit.flatMap(Hit(_)) === hit)

    //Associativity
    val f: Int => Shot[Int] = i => Hit(i + 1)
    val g: Int => Shot[String] = i => Hit(i.toString)

    assert(hit.flatMap(f).flatMap(g) === hit.flatMap(i => f(i).flatMap(g)))
  }

  test("asOpt") {
    assert(hit.asOpt === Some(42))
    assert(miss.asOpt === None)
  }

  test("orElse") {
    val hit1 = Hit(42)
    val hit2 = Hit(99)

    assert((hit1 orElse hit2) === hit1)
    assert((hit2 orElse hit1) === hit2)

    assert((miss orElse hit1) === hit1)
  }

  test("and") {
    import Shots._

    assert((hit and miss) === Shots2(hit, miss))
    assert((miss and hit) === Shots2(miss, hit))
    assert((miss and miss) === Shots2(miss, miss))
    assert((hit and hit) === Shots2(hit, hit))
  }

  test("Miss()") {
    assert(Miss("asdf") === Miss(SnagMessage("asdf")))
  }

  test("sequence()") {
    import Shot.sequence

    val allHits: Traversable[Shot[Int]] = Vector(Hit(1), Hit(2), Hit(99))

    assert(Hit(Vector(1, 2, 99)) == sequence(allHits))

    val someHits: Traversable[Shot[Int]] = Vector(Hit(1), Hit(2), Miss("foo"), Miss("bar"), Hit(99))

    assert(Miss(SnagSeq(Seq(SnagMessage("foo"), SnagMessage("bar")))) == sequence(someHits))

    val allMisses: Traversable[Shot[Int]] = Seq(Miss("foo"), Miss("bar"), Miss("baz"))

    assert(Miss(SnagSeq(Seq(SnagMessage("foo"), SnagMessage("bar"), SnagMessage("baz")))) == sequence(allMisses))

    val empty: Traversable[Shot[Int]] = Nil

    assert(sequence(empty) == Hit(Nil))
  }

  test("Shots.findHit") {
    val divideEvenNumberByTwo: Int => Shot[Int] = i => if (i % 2 == 0) Hit(i / 2) else Miss("Odd number")
    assert(Shots.findHit(Seq(1, 2, 3, 4, 5), divideEvenNumberByTwo) === Hit(1))
    assert(Shots.findHit(Seq(1, 3, 5), divideEvenNumberByTwo) ===
      Miss(SnagSeq(Seq(SnagMessage("Odd number"), SnagMessage("Odd number"), SnagMessage("Odd number")))))
    assert(Shots.findHit(Seq.empty[Int], divideEvenNumberByTwo) === Miss(SnagMessage("List of items is empty.")))
  }

  //scalastyle:off magic.number
}