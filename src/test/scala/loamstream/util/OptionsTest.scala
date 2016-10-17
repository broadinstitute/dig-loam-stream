package loamstream.util

import org.scalatest.FunSuite
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import org.scalatest.Matchers


/**
 * @author clint
 * Oct 17, 2016
 */
final class OptionsTest extends FunSuite with Matchers {
  test("toTry") {
    val msg = "foo!"
    
    import Options.toTry
    
    //test that Some => Success, and that failure messages are evaluated lazily
    toTry(Some("asdf"))(???) shouldEqual Success("asdf")
    
    toTry(None)(msg) shouldBe a[Failure[_]]
    
    val Failure(e) = toTry(None)(msg)
    
    e.getMessage shouldEqual(msg)
  }
}