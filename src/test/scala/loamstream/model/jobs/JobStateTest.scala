package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.util.TypeBox

/**
 * @author clint
 * date: Sep 30, 2016
 */
final class JobStateTest extends FunSuite {
  import JobState._
  
  //scalastyle:off magic.number
  
  test("isSuccess") {
    assert(NotStarted.isSuccess === false)
    assert(Running.isSuccess === false)
    assert(Failed.isSuccess === false)
    assert(Succeeded.isSuccess === true)
    assert(Skipped.isSuccess === true)
    assert(Unknown.isSuccess === false)
  
    assert(CommandResult(0).isSuccess === true)
    
    assert(CommandResult(1).isSuccess === false)
    assert(CommandResult(-1).isSuccess === false)
    assert(CommandResult(42).isSuccess === false)

    assert(FailedWithException(new Exception).isSuccess === false)
  
    assert(ValueSuccess(42, TypeBox.of[Int]).isSuccess === true)
  }
  
  test("isFailure") {
    assert(NotStarted.isFailure === false)
    assert(Running.isFailure === false)
    assert(Failed.isFailure === true)
    assert(Succeeded.isFailure === false)
    assert(Skipped.isFailure === false)
    assert(Unknown.isFailure === false)
  
    assert(CommandResult(0).isFailure === false)
    
    assert(CommandResult(1).isFailure === true)
    assert(CommandResult(-1).isFailure === true)
    assert(CommandResult(42).isFailure === true)

    assert(FailedWithException(new Exception).isFailure === true)
  
    assert(ValueSuccess(42, TypeBox.of[Int]).isFailure === false)
  }
  
  test("isFinished") {
    assert(NotStarted.isFinished === false)
    assert(Running.isFinished === false)
    assert(Failed.isFinished === true)
    assert(Succeeded.isFinished === true)
    assert(Skipped.isFinished === true)
    assert(Unknown.isFinished === false)
  
    assert(CommandResult(0).isFinished === true)
    
    assert(CommandResult(1).isFinished === true)
    assert(CommandResult(-1).isFinished === true)
    assert(CommandResult(42).isFinished === true)

    assert(FailedWithException(new Exception).isFinished === true)
  
    assert(ValueSuccess(42, TypeBox.of[Int]).isFinished === true)
  }
  
  //scalastyle:on magic.number
}