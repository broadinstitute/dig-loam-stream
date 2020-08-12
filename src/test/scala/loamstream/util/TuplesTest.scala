package loamstream.util

import org.scalatest.FunSuite


/**
 * @author clint
 * Jul 24, 2020
 */
final class TuplesTest extends FunSuite {
  
  import Tuples.Implicits.Tuple2Ops
  
  test("mapFirst") {
    val t = (42, "abc")
    
    assert(t.mapFirst(_.toString) === ("42", "abc"))
  }
  
  test("mapSecond") {
    val t = (42, "abc")
    
    assert(t.mapSecond(_.size) === (42, 3))
  }
}
