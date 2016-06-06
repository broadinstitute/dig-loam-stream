package loamstream.model.values

import org.scalatest.FunSuite

import scala.reflect.runtime.universe.typeOf

/**
  * LoamStream
  * Created by oliverr on 6/6/2016.
  */
class LTypeAnalyzerTest extends FunSuite {
  test("Detect Map") {
    assert(LTypeAnalyzer.isMap(typeOf[Map[String, Int]]))
    assert(LTypeAnalyzer.isMap(typeOf[scala.collection.mutable.Map[Double, Int]]))
    assert(LTypeAnalyzer.isMap(typeOf[scala.collection.mutable.Map[Double, Int]]))
  }
}
