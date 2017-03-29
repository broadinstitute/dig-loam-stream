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
    assert(Success.isSuccess === true)
    
    assert(CommandResult(0).isSuccess === true)
    
    assert(Failure.isSuccess === false)
    
    assert(CommandResult(1).isSuccess === false)
    assert(CommandResult(-1).isSuccess === false)
    assert(CommandResult(42).isSuccess === false)

    assert(FailureWithException(new Exception).isSuccess === false)
  
    assert(ValueSuccess(42, TypeBox.of[Int]).isSuccess === true)
  }
  
  test("isFailure") {
    assert(Success.isFailure === false)

    assert(CommandResult(0).isFailure === false)

    assert(Failure.isFailure === true)

    assert(CommandResult(1).isFailure === true)
    assert(CommandResult(-1).isFailure === true)
    assert(CommandResult(42).isFailure === true)

    assert(FailureWithException(new Exception).isFailure === false)

    assert(ValueSuccess(42, TypeBox.of[Int]).isFailure === true)
  }
  
  test("isSuccessExitCode") {
    assert(isSuccessExitCode(0))
    assert(!isSuccessExitCode(1))
    assert(!isSuccessExitCode(-1))
    assert(!isSuccessExitCode(42))
  }

  test("toJobStatus") {
    assert(toJobStatus(0) === JobStatus.Succeeded)
    assert(toJobStatus(1) === JobStatus.Failed)
    assert(toJobStatus(-1) === JobStatus.Failed)
    assert(toJobStatus(42) === JobStatus.Failed)
  }

  //scalastyle:on magic.number
}
