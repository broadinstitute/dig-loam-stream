package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Jun 1, 2016
 */
final class IteratorEnrichmentsTest extends FunSuite {
  test("takeUntil") {
    import IteratorEnrichments._
    
    def isEven(i: Int): Boolean = i % 2 == 0
    
    def isOdd(i: Int): Boolean = !isEven(i)
    
    def empty = Seq.empty[Int].iterator
    
    assert(empty.takeUntil(isEven).toIndexedSeq == Nil)
    assert(empty.takeUntil(isOdd).toIndexedSeq == Nil)
    
    def allEven = Seq(2,4,6,8).iterator
    def allOdd = Seq(1,3,5,7).iterator
    
    assert(allEven.takeUntil(isEven).toIndexedSeq == Seq(2))
    assert(allEven.takeUntil(isOdd).toIndexedSeq == Seq(2,4,6,8))
    
    assert(allOdd.takeUntil(isEven).toIndexedSeq == Seq(1,3,5,7))
    assert(allOdd.takeUntil(isOdd).toIndexedSeq == Seq(1))
    
    def someEven = Seq(2,4,6,7,8,9,10).iterator
    
    assert(someEven.takeUntil(isOdd).toIndexedSeq == Seq(2,4,6,7))
  }
}