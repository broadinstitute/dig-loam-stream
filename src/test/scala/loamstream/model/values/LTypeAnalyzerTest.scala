package loamstream.model.values

import org.scalatest.FunSuite

import scala.collection.immutable.TreeMap
import scala.reflect.runtime.universe.typeOf

/**
  * LoamStream
  * Created by oliverr on 6/6/2016.
  */
class LTypeAnalyzerTest extends FunSuite {
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
  test("Explode type") {
    assert(LTypeAnalyzer.explode(typeOf[String]).head =:= typeOf[String])
    assert(LTypeAnalyzer.explode(typeOf[(String, Int)]).head =:= typeOf[String])
    assert(LTypeAnalyzer.explode(typeOf[(String, Int)])(1) =:= typeOf[Int])
  }
  test("Extract keys") {
    assert(LTypeAnalyzer.keyTypes(typeOf[String]).isEmpty)
    assert(LTypeAnalyzer.keyTypes(typeOf[Set[String]]).size == 1)
    assert(LTypeAnalyzer.keyTypes(typeOf[Set[String]]).head =:= typeOf[String])
    assert(LTypeAnalyzer.keyTypes(typeOf[Set[(String, Int)]]).size == 2)
    assert(LTypeAnalyzer.keyTypes(typeOf[Set[(String, Int)]]).head =:= typeOf[String])
    assert(LTypeAnalyzer.keyTypes(typeOf[Set[(String, Int)]])(1) =:= typeOf[Int])
    assert(LTypeAnalyzer.keyTypes(typeOf[Map[String, Double]]).size == 1)
    assert(LTypeAnalyzer.keyTypes(typeOf[Map[String, Double]]).head =:= typeOf[String])
    assert(LTypeAnalyzer.keyTypes(typeOf[Map[(String, Int), Double]]).size == 2)
    assert(LTypeAnalyzer.keyTypes(typeOf[Map[(String, Int), Double]]).head =:= typeOf[String])
    assert(LTypeAnalyzer.keyTypes(typeOf[Map[(String, Int), Double]])(1) =:= typeOf[Int])
  }
}
