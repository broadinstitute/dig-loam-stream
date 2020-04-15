package loamstream.util

import org.scalatest.FunSuite


/**
 * @author clint
 * Apr 9, 2020
 */
final class IterablesTest extends FunSuite {
  import Iterables.Implicits.IterableOps
  
  test("splitWhen - empty sequence") {
    val is: Seq[Int] = Seq.empty
    
    assert(is.splitWhen(_ => false).toSeq === Nil)
    assert(is.splitWhen(_ => true).toSeq === Nil)
  }
  
  test("splitWhen - trivial predicates") { 
    val is: Seq[Int] = 1 to 10 
    
    assert(is.splitWhen(_ => false).toSeq === Seq(is))
    assert(is.splitWhen(_ => true).toSeq === is.map(_ => Seq.empty))
  }
  
  test("splitWhen - happy path") { 
    val is: Seq[Int] = 1 to 100 
    
    val expected = Seq(
        1 to 9,
        11 to 19,
        21 to 29,
        31 to 39,
        41 to 49,
        51 to 59,
        61 to 69,
        71 to 79,
        81 to 89,
        91 to 99)
    
    def isMultipleOf10(i: Int): Boolean = i % 10 == 0
        
    assert(is.splitWhen(isMultipleOf10).toSeq === expected)
  }
}
