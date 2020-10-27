package loamstream.loam.intake.flip

import org.scalatest.FunSuite

/**
 * @author clint
 * Apr 1, 2020
 */
final class ComplementTest extends FunSuite {
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

    //TODO: What to do instead, if anything?  Bad (non-A,C,G,T) chars are currently left alone. :\ 
    assert(Complement("asdf") === "asdf")
  }
}
