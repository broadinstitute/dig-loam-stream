package loamstream.model.jobs

import org.scalatest.FunSuite

/**
 * @author clint
 * date: May 27, 2016
 */
final class JobTest extends FunSuite {
  test("Result.attempt() (something thrown)") {
    val ex = new Exception("foo")
    
    val failure = LJob.Result.attempt {
      throw ex 
    }
    
    assert(failure == LJob.FailureFromThrowable(ex))
    assert(failure.message == s"Failure! ${ex.getMessage}")
  }
  
  test("Result.attempt() (nothing thrown)") {
    val success = LJob.SimpleSuccess("yay")
    val failure = LJob.SimpleFailure("foo")
    
    assert(LJob.Result.attempt(success) == success)
    
    assert(LJob.Result.attempt(failure) == failure)
  }
}