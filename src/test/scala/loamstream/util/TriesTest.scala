package loamstream.util

import org.scalatest.FunSuite
import scala.util.Failure
import scala.util.Success
import scala.util.Try

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
  
  test("sequence") {
    val s0: Try[String] = Success("x")
    val s1: Try[String] = Success("y")
    val f0: Try[String] = Failure(new Exception("blerg"))
    val f1: Try[String] = Failure(new Exception("zerg"))
    
    import Tries.sequence
    
    assert(sequence(Vector(s0, s1)) === Success(Vector("x", "y")))
    assert(sequence(List(s1, s0)) === Success(List("y", "x")))
    
    assert(sequence(List(f0)) === f0)
    assert(sequence(List(f0, f1)) === f0)
    
    assert(sequence(List(s0, f0, s1)) === f0)
    assert(sequence(List(s0, f1, f0, s1)) === f1)
  }
}
