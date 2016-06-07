package loamstream.model.values

import org.scalatest.FunSuite

import scala.collection.immutable.TreeMap
import scala.reflect.runtime.universe.{Type, TypeTag, typeOf}

/**
  * LoamStream
  * Created by oliverr on 6/6/2016.
  */
class LTypeAnalyzerTest extends FunSuite {

  def areSeqsOfSameTypes(types1: Seq[Type], types2: Seq[Type]): Boolean =
    (types1.size == types2.size) && types1.zip(types2).forall({ case (tpe1, tpe2) => tpe1 =:= tpe2 })

  test("Detect Map") {
    assert(LTypeAnalyzer.isMap(typeOf[Map[String, Int]]))
    assert(LTypeAnalyzer.isMap(typeOf[scala.collection.mutable.Map[Double, Int]]))
    assert(LTypeAnalyzer.isMap(typeOf[TreeMap[Double, Int]]))
    assert(!LTypeAnalyzer.isMap(typeOf[Seq[Double]]))
    assert(!LTypeAnalyzer.isMap(typeOf[Set[Set[String]]]))
    assert(!LTypeAnalyzer.isMap(typeOf[String]))
    assert(!LTypeAnalyzer.isMap(typeOf[(Int, Double, Char)]))
  }
  test("Detect Iterable") {
    assert(LTypeAnalyzer.isIterable(typeOf[Map[String, Int]]))
    assert(LTypeAnalyzer.isIterable(typeOf[scala.collection.mutable.Map[Double, Int]]))
    assert(LTypeAnalyzer.isIterable(typeOf[TreeMap[Double, Int]]))
    assert(LTypeAnalyzer.isIterable(typeOf[Seq[Double]]))
    assert(LTypeAnalyzer.isIterable(typeOf[Set[Set[String]]]))
    assert(!LTypeAnalyzer.isIterable(typeOf[String]))
    assert(!LTypeAnalyzer.isIterable(typeOf[(Int, Double, Char)]))
  }
  test("Detect Product") {
    assert(LTypeAnalyzer.isProduct(typeOf[(String, Int)]))
    assert(LTypeAnalyzer.isProduct(typeOf[(Double, Char, Byte)]))
    assert(LTypeAnalyzer.isProduct(typeOf[((Int, Double), (String, AnyRef))]))
    assert(!LTypeAnalyzer.isProduct(typeOf[String]))
    assert(!LTypeAnalyzer.isProduct(typeOf[Map[String, Int]]))
    assert(!LTypeAnalyzer.isProduct(typeOf[Seq[String]]))
  }

  private class A[T]

  private object A {

    class B

  }

  private case class C(i: Int, s: String, ai: A[Int], ab: A.B, d: Double)

  def explodesLikeThis(tpe: Type, parts: Type*): Boolean =
    areSeqsOfSameTypes(LTypeAnalyzer.explode(tpe), parts)

  test("Explode type") {
    assert(explodesLikeThis(typeOf[String], typeOf[String]))
    assert(explodesLikeThis(typeOf[(String, Int)], typeOf[String], typeOf[Int]))
    assert(explodesLikeThis(typeOf[(Int, String, A[Int], A.B, Double)],
      typeOf[Int], typeOf[String], typeOf[A[Int]], typeOf[A.B], typeOf[Double]))
  }

  def tupleOneExplodesProperly[A: TypeTag](): Boolean = explodesLikeThis(typeOf[Tuple1[A]], typeOf[A])

  test("Product1 explodes to Seq of one") {
    assert(tupleOneExplodesProperly[Int]())
    assert(tupleOneExplodesProperly[String]())
    assert(tupleOneExplodesProperly[C]())
    assert(tupleOneExplodesProperly[A[Double]]())
    assert(tupleOneExplodesProperly[A.B]())
  }

  def nonTupleDoesNotExplode[A: TypeTag](): Boolean = explodesLikeThis(typeOf[A], typeOf[A])

  test("If it is not a tuple, it will not explode.") {
    assert(nonTupleDoesNotExplode[String]())
    assert(nonTupleDoesNotExplode[A[Int]]())
    assert(nonTupleDoesNotExplode[A.B]())
    assert(nonTupleDoesNotExplode[C]())
  }

  def hasTheseKeys(tpe: Type, keys: Type*): Boolean =
    areSeqsOfSameTypes(LTypeAnalyzer.keyTypes(tpe), keys)

  test("Extract keys") {
    assert(hasTheseKeys(typeOf[String]))
    assert(hasTheseKeys(typeOf[Set[String]], typeOf[String]))
    assert(hasTheseKeys(typeOf[Set[(String, Int)]], typeOf[String], typeOf[Int]))
    assert(hasTheseKeys(typeOf[Map[String, Double]], typeOf[String]))
    assert(hasTheseKeys(typeOf[Map[(String, Int), Double]], typeOf[String], typeOf[Int]))
  }

}
