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
}
