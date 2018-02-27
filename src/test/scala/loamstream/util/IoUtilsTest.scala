package loamstream.util

import org.scalatest.FunSuite
import java.io.StringWriter
import java.io.PrintWriter

/**
 * @author clint
 * Feb 27, 2018
 */
final class IoUtilsTest extends FunSuite {
  test("printTo") {
    val stringWriter = new StringWriter
    
    val printWriter = new PrintWriter(stringWriter)
    
    assert(stringWriter.toString === "")
    
    IoUtils.printTo(printWriter)("")
    
    assert(stringWriter.toString === s"${System.lineSeparator}")
    
    IoUtils.printTo(printWriter)("foo")
    
    assert(stringWriter.toString === s"${System.lineSeparator}foo${System.lineSeparator}")
    
    IoUtils.printTo(printWriter)("bar")
    
    assert(stringWriter.toString === s"${System.lineSeparator}foo${System.lineSeparator}bar${System.lineSeparator}")
  }
}
