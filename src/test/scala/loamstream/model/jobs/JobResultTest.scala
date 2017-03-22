package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.util.TypeBox
import loamstream.model.execute.Resources.LocalResources
import loamstream.TestHelpers

/**
 * @author clint
 * date: Sep 30, 2016
 */
final class JobResultTest extends FunSuite {
  import JobResult._
  
  //scalastyle:off magic.number
  
  test("isSuccess") {
    assert(NotStarted.isSuccess === false)
    assert(Running.isSuccess === false)
    assert(Failed().isSuccess === false)
    assert(Failed(Some(TestHelpers.localResources)).isSuccess === false)
    assert(Succeeded.isSuccess === true)
    assert(Skipped.isSuccess === true)
    assert(Unknown.isSuccess === false)
  
    assert(CommandResult(0, Some(TestHelpers.localResources)).isSuccess === true)
    
    assert(CommandResult(1, Some(TestHelpers.localResources)).isSuccess === false)
    assert(CommandResult(-1, Some(TestHelpers.localResources)).isSuccess === false)
    assert(CommandResult(42, Some(TestHelpers.localResources)).isSuccess === false)

    assert(FailedWithException(new Exception).isSuccess === false)
  
    assert(ValueSuccess(42, TypeBox.of[Int]).isSuccess === true)
  }
  
  test("isFailure") {
    assert(NotStarted.isFailure === false)
    assert(Running.isFailure === false)
    assert(Failed().isFailure === true)
    assert(Failed(Some(TestHelpers.localResources)).isFailure === true)
    assert(Succeeded.isFailure === false)
    assert(Skipped.isFailure === false)
    assert(Unknown.isFailure === false)
  
    assert(CommandResult(0, Some(TestHelpers.localResources)).isFailure === false)
    
    assert(CommandResult(1, Some(TestHelpers.localResources)).isFailure === true)
    assert(CommandResult(-1, Some(TestHelpers.localResources)).isFailure === true)
    assert(CommandResult(42, Some(TestHelpers.localResources)).isFailure === true)

    assert(FailedWithException(new Exception).isFailure === true)
  
    assert(ValueSuccess(42, TypeBox.of[Int]).isFailure === false)
  }
  
  test("isFinished") {
    assert(NotStarted.isFinished === false)
    assert(Running.isFinished === false)
    assert(Failed().isFinished === true)
    assert(Failed(Some(TestHelpers.localResources)).isFinished === true)
    assert(Succeeded.isFinished === true)
    assert(Skipped.isFinished === true)
    assert(Unknown.isFinished === false)
  
    assert(CommandResult(0, Some(TestHelpers.localResources)).isFinished === true)
    
    assert(CommandResult(1, Some(TestHelpers.localResources)).isFinished === true)
    assert(CommandResult(-1, Some(TestHelpers.localResources)).isFinished === true)
    assert(CommandResult(42, Some(TestHelpers.localResources)).isFinished === true)

    assert(FailedWithException(new Exception).isFinished === true)
  
    assert(ValueSuccess(42, TypeBox.of[Int]).isFinished === true)
  }
  
  //scalastyle:on magic.number
}
