package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Aug 11, 2016
 */
final class TraversablesTest extends FunSuite {
  test("mapTo") {
    val as = Seq("a", "bb", "asdfghj")
    
    import Traversables.Implicits._
    
    val map = as.mapTo(_.size)
    
    assert(map == Map("a" -> 1, "bb" -> 2, "asdfghj" -> 7))
  }
}