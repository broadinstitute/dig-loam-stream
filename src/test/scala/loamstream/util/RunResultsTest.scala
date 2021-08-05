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
    
    val s = RunResults.Completed("foo", 0, Nil, Nil)
    
    assert(s.tryAsSuccess("", RunResults.SuccessPredicate.zeroIsSuccess) === Success(s))
    assert(s.tryAsSuccess("", RunResults.SuccessPredicate.countsAsSuccess(42)).isFailure)
    
    val u = RunResults.Completed("foo", 42, Nil, Nil)
    
    assert(u.tryAsSuccess("", RunResults.SuccessPredicate.zeroIsSuccess).isFailure === true)
    assert(u.tryAsSuccess("", RunResults.SuccessPredicate.countsAsSuccess(42)) === Success(u))
    
    val cns = RunResults.CouldNotStart("foo", new Exception with NoStackTrace)
    
    assert(cns.tryAsSuccess("", RunResults.SuccessPredicate.zeroIsSuccess).isFailure)
    assert(cns.tryAsSuccess("", RunResults.SuccessPredicate.countsAsSuccess(42)).isFailure)
  }
  
  test("apply - defaults") {
    assert(RunResults("foo", 0, Nil, Nil) === RunResults.Completed("foo", 0, Nil, Nil))
    assert(RunResults("foo", 42, Nil, Nil) === RunResults.Completed("foo", 42, Nil, Nil))
  }
  
  test("apply - non-default exit code handling") {
    def zeroOr42IsSuccess(ec: Int) = ec == 0 || ec == 42
    
    assert(RunResults("foo", 0, Nil, Nil) === RunResults.Completed("foo", 0, Nil, Nil))
    assert(RunResults("foo", 42, Nil, Nil) === RunResults.Completed("foo", 42, Nil, Nil))
    assert(RunResults("foo", 99, Nil, Nil) === RunResults.Completed("foo", 99, Nil, Nil))
  }
}
