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
  test("toTry - default failure") {
    val msg = "foo!"
    
    import Options.toTry
    
    //test that Some => Success, and that failure messages are evaluated lazily
    toTry(Some("asdf"))(???) shouldEqual Success("asdf")
    
    toTry(None)(msg) shouldBe a[Failure[_]]
    
    val Failure(e) = toTry(None)(msg)
    
    e.getMessage shouldEqual(msg)
  }
  
  test("toTry - specific failure") {
    val msg = "foo!"
    
    import Options.toTry
    
    def makeException(message: String) = new IllegalArgumentException(message)
    
    //test that Some => Success, and that failure messages are evaluated lazily
    toTry(Some("asdf"))(???, makeException) shouldEqual Success("asdf")
    
    toTry(None)(msg, makeException) shouldBe a[Failure[_]]
    
    val Failure(e) = toTry(None)(msg, makeException)
    
    e.getMessage shouldEqual(msg)
    e.getClass shouldEqual(classOf[IllegalArgumentException])
  }
}
