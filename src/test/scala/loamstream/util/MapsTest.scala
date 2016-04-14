package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Apr 13, 2016
 */
final class MapsTest extends FunSuite {
  private def empty: Map[Int, String] = Map.empty

  test("mergeMaps()") {
    import Maps.mergeMaps

    assert(mergeMaps(Seq.empty[Map[Int, String]]) === empty)
    assert(mergeMaps(Seq(empty)) === empty)
    assert(mergeMaps(Seq(empty, empty)) === empty)
    assert(mergeMaps(Seq(empty, empty, empty)) === empty)

    val m0 = Map(1 -> "a")

    assert(mergeMaps(Seq(m0)) === m0)
    assert(mergeMaps(Seq(empty, m0)) === m0)
    assert(mergeMaps(Seq(m0, empty, empty)) === m0)
    assert(mergeMaps(Seq(empty, m0, empty)) === m0)

    val m1 = Map(2 -> "b")

    val m01 = Map(1 -> "a", 2 -> "b")

    assert(mergeMaps(Seq(m0, m1)) === m01)
    assert(mergeMaps(Seq(m1, m0)) === m01)
    assert(mergeMaps(Seq(m1, empty, m0)) === m01)
    assert(mergeMaps(Seq(m0, empty, empty, m1)) === m01)
    assert(mergeMaps(Seq(empty, m0, empty, m1)) === m01)

    val m00 = Map(1 -> "X")

    assert(mergeMaps(Seq(m0, m00)) === Map(1 -> "X"))
    assert(mergeMaps(Seq(m00, m0)) === Map(1 -> "a"))

    val m2 = Map(3 -> "c")

    val m012 = Map(1 -> "a", 2 -> "b", 3 -> "c")

    assert(mergeMaps(Seq(m0, m1, m2)) === m012)
    assert(mergeMaps(Seq(m1, m0, m2)) === m012)
    assert(mergeMaps(Seq(m2, m1, m0)) === m012)
  }

  test("strictMapValues()") {
    val m = Map("a" -> 0, "b" -> 1, "c" -> 2)

    var invocations = 0
    
    import Maps.Implicits._

    val mapped = m.strictMapValues { v =>
      invocations += 1

      v + 1
    }
    
    val expected = Map("a" -> 1, "b" -> 2, "c" -> 3)
    
    assert(mapped === expected) 

    assert(mapped.values.sum === 6)
    assert(mapped.values.sum === 6)
    assert(mapped.values.sum === 6)

    //If we'd used .mapValues(), invocations would be 9
    assert(invocations === 3)
  }
}