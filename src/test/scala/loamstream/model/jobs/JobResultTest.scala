package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.util.TypeBox

/**
 * @author clint
 * date: Sep 30, 2016
 */
final class JobResultTest extends FunSuite {
  import JobResult._
  
  // scalastyle:off magic.number
  
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

    assert(FailureWithException(new Exception).isFailure === true)

    assert(ValueSuccess(42, TypeBox.of[Int]).isFailure === false)
  }
  
  test("toJobStatus") {
    assert(Success.toJobStatus === JobStatus.Succeeded)
    
    assert(CommandResult(0).toJobStatus === JobStatus.Succeeded)
    
    assert(Failure.toJobStatus === JobStatus.Failed)
    
    assert(CommandResult(1).toJobStatus === JobStatus.Failed)
    assert(CommandResult(-1).toJobStatus === JobStatus.Failed)
    assert(CommandResult(42).toJobStatus === JobStatus.Failed)

    assert(FailureWithException(new Exception).toJobStatus === JobStatus.Failed)
  
    assert(ValueSuccess(42, TypeBox.of[Int]).toJobStatus === JobStatus.Succeeded)
  }
  
  // scalastyle:on magic.number
}
