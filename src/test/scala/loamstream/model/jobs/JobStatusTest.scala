package loamstream.model.jobs

import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 3/29/17
 */
final class JobStatusTest extends FunSuite {
  import JobStatus._
  
  test("isSuccess") {
    assert(Succeeded.isSuccess === true)
    assert(Skipped.isSuccess === true)
    assert(Failed.isSuccess === false)
    assert(FailedWithException.isSuccess === false)
    assert(NotStarted.isSuccess === false)
    assert(Submitted.isSuccess === false)
    assert(Running.isSuccess === false)
    assert(Terminated.isSuccess === false)
    assert(Unknown.isSuccess === false)
  }

  test("isFailure") {
    assert(Succeeded.isFailure === false)
    assert(Skipped.isFailure === false)
    assert(Failed.isFailure === true)
    assert(FailedWithException.isFailure === true)
    assert(NotStarted.isFailure === false)
    assert(Submitted.isFailure === false)
    assert(Running.isFailure === false)
    assert(Terminated.isFailure === true)
    assert(Unknown.isFailure === false)
  }

  test("isFinished") {
    assert(Succeeded.isFinished === true)
    assert(Skipped.isFinished === true)
    assert(Failed.isFinished === true)
    assert(FailedWithException.isFinished === true)
    assert(NotStarted.isFinished === false)
    assert(Submitted.isFinished === false)
    assert(Running.isFinished === false)
    assert(Terminated.isFinished === true)
    assert(Unknown.isFinished === false)
  }

  test("notFinished") {
    assert(Succeeded.notFinished === false)
    assert(Skipped.notFinished === false)
    assert(Failed.notFinished === false)
    assert(FailedWithException.notFinished === false)
    assert(NotStarted.notFinished === true)
    assert(Submitted.notFinished === true)
    assert(Running.notFinished === true)
    assert(Terminated.notFinished === false)
    assert(Unknown.notFinished === true)
  }
  
  test("fromString") {
    assert(fromString("Succeeded") === Some(Succeeded))
    assert(fromString("Skipped") === Some(Skipped))
    assert(fromString("Failed") === Some(Failed))
    assert(fromString("FailedWithException") === Some(FailedWithException))
    assert(fromString("NotStarted") === Some(NotStarted))
    assert(fromString("Submitted") === Some(Submitted))
    assert(fromString("Running") === Some(Running))
    assert(fromString("Terminated") === Some(Terminated))
    assert(fromString("Unknown") === Some(Unknown))
    assert(fromString("") === None)
    assert(fromString("Undefined") === None)
    assert(fromString("blah") === None)
  }

  test("fromExitCode") {
    assert(fromExitCode(0) === Succeeded)
    assert(fromExitCode(1) === Failed)
    assert(fromExitCode(-1) === Failed)
    assert(fromExitCode(42) === Failed) // scalastyle:ignore magic.number
  }

}
