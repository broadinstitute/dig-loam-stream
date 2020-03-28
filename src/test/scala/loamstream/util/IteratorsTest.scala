package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Sep 25, 2017
 */
final class IteratorsTest extends FunSuite {
  test("nextOption") {
    import Iterators.Implicits._
    
    val empty: Iterator[Int] = Iterator.empty
    
    assert(empty.hasNext === false)
    assert(empty.nextOption === None)
    
    val initiallyNonEmpty = Iterator(42, 99, 100)
    
    assert(initiallyNonEmpty.hasNext)
    
    assert(initiallyNonEmpty.nextOption === Some(42))
    assert(initiallyNonEmpty.nextOption === Some(99))
    assert(initiallyNonEmpty.nextOption === Some(100))
    assert(initiallyNonEmpty.nextOption === None)
  }
  
  test("deltasBetween") {
    import Iterators.deltasBetween
    
    assert(deltasBetween(Nil).toIndexedSeq === Nil)
    
    assert(deltasBetween(1 to 5).toIndexedSeq === Seq(1,1,1,1))
    
    assert(deltasBetween(Seq(1, 2, 4, 7, 11)).toIndexedSeq === Seq(1,2,3,4))
    assert(deltasBetween(Seq(11, 7, 4, 2, 1)).toIndexedSeq === Seq(-4, -3, -2, -1))
  }
  
  test("sample") {
    import Iterators.sample
    
    assert(sample(Iterator.empty, Seq(1,2,3,4)).toIndexedSeq === Iterator.empty.toIndexedSeq)
    
    assert(sample(Iterator('a','b','c','d','e','f','g','h'), Seq(1, 2, 4, 6)).toIndexedSeq === Seq('b','c','e','g'))
  }
}
