package loamstream.loam.intake.flip

import org.scalatest.FunSuite
import loamstream.TestHelpers
import java.nio.file.Path
import java.util.UUID
import loamstream.util.Files
import org.apache.commons.io.FileUtils
import org.scalactic.source.Position.apply


/**
 * @author clint
 * Apr 1, 2020
 */
final class ReferenceFileHandleTest extends FunSuite {
  import Helpers.withTestFile
  
  test("readAt(i)") {
    withTestFile("0123456789") { testFile =>
      val handle = ReferenceFileHandle(testFile.toFile)
      
      intercept[Exception] {
        handle.readAt(-100) === None
      }
      
      intercept[Exception] {
        handle.readAt(-1) === None
      }
      
      assert(handle.readAt(0) === Some('0'))
      assert(handle.readAt(5) === Some('5'))
      assert(handle.readAt(1) === Some('1'))
      assert(handle.readAt(9) === Some('9'))
      
      //EOF
      assert(handle.readAt(10) === None) 
      assert(handle.readAt(100) === None)
    }
  }
  
  test("readAt(i, length)") {
    withTestFile("0123456789") { testFile =>
      val handle = ReferenceFileHandle(testFile.toFile)
      
      intercept[Exception] {
        handle.readAt(-100, 2) === None
      }
      
      intercept[Exception] {
        handle.readAt(-1, 42) === None
      }
      
      assert(handle.readAt(0, 1) === Some("0"))
      assert(handle.readAt(5, 1) === Some("5"))
      assert(handle.readAt(1, 1) === Some("1"))
      assert(handle.readAt(9, 1) === Some("9"))
      
      //EOF
      assert(handle.readAt(10, 1) === None) 
      assert(handle.readAt(100) === None)
    }
  }
}
