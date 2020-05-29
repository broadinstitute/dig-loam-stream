package loamstream.loam.intake.flip

import org.scalatest.FunSuite
import loamstream.TestHelpers

/**
 * @author clint
 * Apr 7, 2020
 */
final class ReferenceFilesTest extends FunSuite {
  import TestHelpers.path
  
  test("isKnown") {
    val files = ReferenceFiles(path("src/test/resources/intake/reference-first-1M-of-chrom1"), Set("1"))
    
    assert(files.isKnown("1") === true)
    assert(files.isKnown("2") === false)
    assert(files.isKnown("11") === false)
    assert(files.isKnown("X") === false)
    assert(files.isKnown("Y") === false)
  }

  test("getChar") {
    val files = ReferenceFiles(path("src/test/resources/intake/reference-bogus"), Set("1", "X"))
    
    assert(files.getChar("2", 1) === None)
    assert(files.getChar("12", 1) === None)
    assert(files.getChar("Y", 1) === None)
    
    assert(files.getChar("1", 0) === Some('A'))
    assert(files.getChar("1", 1) === Some('B'))
    assert(files.getChar("1", 2) === Some('C'))
    
    assert(files.getChar("X", 0) === Some('C'))
    assert(files.getChar("X", 1) === Some('B'))
    assert(files.getChar("X", 2) === Some('A'))
  }
  
  test("getString") {
    val files = ReferenceFiles(path("src/test/resources/intake/reference-bogus"), Set("1", "X"))
    
    assert(files.getString("2", 1, 1) === None)
    assert(files.getString("12", 1, 1) === None)
    assert(files.getString("Y", 1, 1) === None)
    
    assert(files.getString("1", 0, 1) === Some("A"))
    assert(files.getString("1", 1, 2) === Some("BC"))
    assert(files.getString("1", 0, 3) === Some("ABC"))
    
    assert(files.getString("1", 0, 1) === Some("A"))
    assert(files.getString("1", 1, 2) === Some("BC"))
    assert(files.getString("1", 0, 10) === None)
    
    assert(files.getString("X", 0, 1) === Some("C"))
    assert(files.getString("X", 1, 2) === Some("BA"))
    assert(files.getString("X", 0, 3) === Some("CBA"))
    assert(files.getString("X", 0, 99) === None)
  }
}
