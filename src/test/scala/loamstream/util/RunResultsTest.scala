package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * May 18, 2018
 */
final class RunResultsTest extends FunSuite {
  
  private val success = RunResults("foo", 0, "abc".map(_.toString), "asdf".map(_.toString))
  
  private val failure = RunResults("foo", 42, "abc".map(_.toString), "asdf".map(_.toString))
  
  test("isSuccess/isFailure") {
    assert(success.isSuccess)
    assert(success.isFailure === false)
    
    assert(failure.isSuccess === false)
    assert(failure.isFailure)
  }
}
