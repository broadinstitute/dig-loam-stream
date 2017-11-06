package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.quantities.Memory

/**
 * @author clint
 * Mar 13, 2017
 */
final class MemoryTest extends FunSuite {
  //scalastyle:off magic.number
  
  import Memory.inGb
  import Memory.inBytes
  
  private val zero = inBytes(0)
  
  test("Guards") {
    intercept[Exception] {
      inBytes(-1)
    }
    
    intercept[Exception] {
      inBytes(-1024)
    }
    
    intercept[Exception] {
      inBytes(Long.MinValue)
    }
    
    inBytes(0)
    inBytes(1)
    inBytes(1024)
    inBytes(Long.MaxValue)
  }
  
  test("inGb") {
    assert(inGb(0).value.toBytes === 0L)
    assert(inGb(1).value.toBytes === 1000000000L)
    assert(inGb(2).value.toBytes === 2000000000L)
  }
  
  test("gb, mb, kb") {
    assert(zero.kb === 0)
    assert(zero.mb === 0)
    assert(zero.gb === 0)

    def doTestFromGigs(howMany: Int): Unit = {
      val gigs = inGb(howMany)
    
      assert(gigs.gb === howMany)
      assert(gigs.mb === (howMany * 1000))
      assert(gigs.kb === (howMany * 1000000))
    }
    
    doTestFromGigs(1)
    doTestFromGigs(2)
    doTestFromGigs(42)
  }
  
  test("*") {
    assert(zero * 0 === zero)
    assert(zero * -1 === zero)
    assert(zero * 1 === zero)
    assert(zero * 42 === zero)
    
    val oneGig = inGb(1)
    
    assert(oneGig * 1 === oneGig)
    assert(oneGig * 2 === inGb(2))
    assert(oneGig * 42 === inGb(42))
  }
  
  test("/") {
    intercept[Exception] {
      zero / 0
    }
    
    assert(zero / -1 === zero)
    assert(zero / 1 === zero)
    assert(zero / 42 === zero)
    
    val twoGigs = inGb(2)
    
    intercept[Exception] {
      twoGigs / 0
    }
    
    assert(twoGigs / 1 === twoGigs)
    assert(twoGigs / 2 === inGb(1))
    assert(inGb(42) / 2 === inGb(21))
    assert(inGb(100) / 25 === inGb(4))
  }
  
  test("double") {
    assert(zero.double === zero)
    
    assert(inGb(1).double === inGb(2))
    
    assert(inGb(2).double === inGb(4))
    
    assert(inGb(4).double === inGb(8))
  }
  
  //scalastyle:on magic.number
}
