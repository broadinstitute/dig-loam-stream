package loamstream.util

import org.scalatest.FunSuite

import scala.collection.compat._

/**
 * @author clint
 * date: Aug 12, 2016
 */
final class SequenceTest extends FunSuite {
  //scalastyle:off magic.number
  test("Int and Long Sequences, using defaults") {
    {
      val is: Sequence[Int] = Sequence()
      
      val first5 = is.iterator.take(5).to(List)
      
      assert(first5 == Seq(0,1,2,3,4))
    }
    
    {
      val ls: Sequence[Long] = Sequence()
      
      val first5 = ls.iterator.take(5).to(List)
      
      assert(first5 == Seq(0L,1L,2L,3L,4L))
    }
  }
  
  test("Int and Long Sequences, specified start and step") {
    {
      val is: Sequence[Int] = Sequence(5, 3)
      
      val first5 = is.iterator.take(5).to(List)
      
      assert(first5 == Seq(5,8,11,14,17))
    }
    
    {
      val ls: Sequence[Long] = Sequence(5L, 3L)
      
      val first5 = ls.iterator.take(5).to(List)
      
      assert(first5 == Seq(5L,8L,11L,14L,17L))
    }
  }
  
  test("Int Sequence, specified start and step (negative)") {
    {
      val is: Sequence[Int] = Sequence(-5, 3)
      
      val first5 = is.iterator.take(5).to(List)
      
      assert(first5 == Seq(-5,-2,1,4,7))
    }
    
    {
      val is: Sequence[Int] = Sequence(5, -3)
      
      val first5 = is.iterator.take(5).to(List)
      
      assert(first5 == Seq(5,2,-1,-4,-7))
    }
    
    {
      val is: Sequence[Int] = Sequence(-5, -3)
      
      val first5 = is.iterator.take(5).to(List)
      
      assert(first5 == Seq(-5,-8,-11,-14,-17))
    }
  }
  //scalastyle:on magic.number
}