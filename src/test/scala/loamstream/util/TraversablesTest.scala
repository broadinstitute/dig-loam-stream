package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Aug 11, 2016
 */
final class TraversablesTest extends FunSuite {
  val as = Seq("a", "bb", "asdfghj")

  import Traversables.Implicits._

  test("mapTo") {
    val map = as.mapTo(_.size)

    assert(map == Map("a" -> 1, "bb" -> 2, "asdfghj" -> 7))
  }

  test("flatMapTo") {
    def f(a: String): Option[Int] = if (a.startsWith("a")) Some(a.size) else None

    val map = as.flatMapTo(f)

    assert(map == Map("a" -> 1, "asdfghj" -> 7))
  }
}