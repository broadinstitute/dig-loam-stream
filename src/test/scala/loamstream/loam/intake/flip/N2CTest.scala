package loamstream.loam.intake.flip

import org.scalatest.FunSuite

/**
 * @author clint
 * Apr 1, 2020
 */
final class N2CTest extends FunSuite {
  test("apply") {
    assert(Complement("A") === "T")
    assert(Complement("C") === "G")
    assert(Complement("T") === "A")
    assert(Complement("G") === "C")
    
    intercept[Exception] {
      Complement("B")
    }
    
    intercept[Exception] {
      Complement("D")
    }
    
    intercept[Exception] {
      Complement("Z")
    }
    
    intercept[Exception] {
      Complement("")
    }
    
    intercept[Exception] {
      Complement("asdf")
    }
  }
}
