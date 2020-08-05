package loamstream.util

import org.scalatest.FunSuite
import scala.util.Success
import scala.util.control.NoStackTrace

/**
 * @author clint
 * Aug 5, 2020
 */
final class RunResultsTest extends FunSuite {
  test("tryAsSuccess") {
    implicit val logCtx = LogContext.Noop
    
    val s = RunResults.Successful("foo", Nil, Nil)
    
    assert(s.tryAsSuccess === Success(s))
    
    val u = RunResults.Unsuccessful("foo", 42, Nil, Nil)
    
    assert(u.tryAsSuccess.isFailure)
    
    val cns = RunResults.CouldNotStart("foo", new Exception with NoStackTrace)
    
    assert(cns.tryAsSuccess.isFailure)
  }
  
  test("apply - defaults") {
    assert(RunResults("foo", 0, Nil, Nil) === RunResults.Successful("foo", Nil, Nil))
    assert(RunResults("foo", 42, Nil, Nil) === RunResults.Unsuccessful("foo", 42, Nil, Nil))
  }
  
  test("apply - non-default exit code handling") {
    def zeroOr42IsSuccess(ec: Int) = ec == 0 || ec == 42
    
    assert(RunResults("foo", 0, Nil, Nil, zeroOr42IsSuccess) === RunResults.Successful("foo", Nil, Nil))
    assert(RunResults("foo", 42, Nil, Nil, zeroOr42IsSuccess) === RunResults.Successful("foo", Nil, Nil))
    assert(RunResults("foo", 99, Nil, Nil) === RunResults.Unsuccessful("foo", 99, Nil, Nil))
  }
}
