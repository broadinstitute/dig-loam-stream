package loamstream.util

import org.scalatest.FunSuite
import scala.util.Failure

/**
 * @author clint
 * Mar 15, 2017
 */
final class TriesTest extends FunSuite {
  import Tries.failure
  
  test("failure, default Exception") {
    val f = failure("foo")
    
    val Failure(e) = f
    
    assert(e.getMessage === "foo")
    assert(e.getCause === null) //scalastyle:ignore null
    assert(e.getClass === classOf[Exception])
  }
  
  test("failure, specified Exception") {
    val f = failure("foo", new IllegalArgumentException(_))
    
    val Failure(e) = f
    
    assert(e.getMessage === "foo")
    assert(e.getCause === null) //scalastyle:ignore null
    assert(e.getClass === classOf[IllegalArgumentException])
  }
}
