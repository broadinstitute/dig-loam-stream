package loamstream.loam.intake.flip

import org.scalatest.FunSuite

/**
 * @author clint
 * Apr 1, 2020
 */
final class N2CTest extends FunSuite {
  test("apply") {
    assert(N2C("A") === "T")
    assert(N2C("C") === "G")
    assert(N2C("T") === "A")
    assert(N2C("G") === "C")
    
    intercept[Exception] {
      N2C("B")
    }
    
    intercept[Exception] {
      N2C("D")
    }
    
    intercept[Exception] {
      N2C("Z")
    }
    
    intercept[Exception] {
      N2C("")
    }
    
    intercept[Exception] {
      N2C("asdf")
    }
  }
}
