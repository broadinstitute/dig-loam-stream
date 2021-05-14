package loamstream.util

import org.scalatest.FunSuite

import scala.collection.compat._

/**
 * @author clint
 * date: Aug 11, 2016
 */
final class TraversablesTest extends FunSuite {
  
  val as = Seq("a", "bb", "asdfghj")

  import Traversables.Implicits._

  //scalastyle:off magic.number
  
  test("splitOn") {
    import Traversables.Implicits._
    
    def split(t: Traversable[Char]): List[String] = t.splitOn(_ == '-').to(List).map(_.mkString)
    
    assert(split("abc-tuv-xyz-".toTraversable) === List("abc", "tuv", "xyz"))
    assert(split("-abc-tuv-xyz".toTraversable) === List("abc", "tuv", "xyz"))
    
    assert(split(Nil) === Nil)
    
    assert(split("abctuvxyz".toTraversable) === List("abctuvxyz"))
    
    assert(split("abctuvxyz-".toTraversable) === List("abctuvxyz"))
    
    assert(split("---".toTraversable) === Nil)
  }
  
  test("mapTo") {
    val map = as.mapTo(_.size)

    assert(map == Map("a" -> 1, "bb" -> 2, "asdfghj" -> 7))
  }

  test("flatMapTo") {
    def f(a: String): Option[Int] = if (a.startsWith("a")) Some(a.size) else None

    val map = as.flatMapTo(f)

    assert(map == Map("a" -> 1, "asdfghj" -> 7))
  }
  
  test("mapBy") {
    final case class Point(x: Int, y: Int)
    
    assert(Seq.empty[Point].mapBy(identity) === Map.empty)

    val points = Seq(Point(42, 2), Point(99, 9))
    
    assert(points.mapBy(_.x) === Map(42 -> Point(42, 2), 99 -> Point(99, 9)))
    
    assert(points.mapBy(_.y) === Map(2 -> Point(42, 2), 9 -> Point(99, 9)))
  }
  //scalastyle:on magic.number
}
