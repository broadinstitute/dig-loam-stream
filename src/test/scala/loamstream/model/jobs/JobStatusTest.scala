package loamstream.model.jobs

import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 3/29/17
 */
final class JobStatusTest extends FunSuite {
  import JobStatus._
  
  test("isPermanentFailure") {
    assert(Succeeded.isPermanentFailure === false)
    assert(Skipped.isPermanentFailure === false)
    assert(Failed.isPermanentFailure === false)
    assert(FailedWithException.isPermanentFailure === false)
    assert(NotStarted.isPermanentFailure === false)
    assert(Submitted.isPermanentFailure === false)
    assert(Running.isPermanentFailure === false)
    assert(Terminated.isPermanentFailure === false)
    assert(Unknown.isPermanentFailure === false)
    assert(PermanentFailure.isPermanentFailure === true)
  }
  
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
    assert(PermanentFailure.isSuccess === false)
  }
  
  test("isSkipped") {
    assert(Succeeded.isSkipped === false)
    assert(Skipped.isSkipped === true)
    assert(Failed.isSkipped === false)
    assert(FailedWithException.isSkipped === false)
    assert(NotStarted.isSkipped === false)
    assert(Submitted.isSkipped === false)
    assert(Running.isSkipped === false)
    assert(Terminated.isSkipped === false)
    assert(Unknown.isSkipped === false)
    assert(PermanentFailure.isSkipped === false)
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
    assert(PermanentFailure.isFailure === true)
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
    assert(PermanentFailure.isFinished === true)
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
    assert(PermanentFailure.notFinished === false)
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
    assert(fromString("PermanentFailure") === Some(PermanentFailure))
    assert(fromString("") === None)
    assert(fromString("Undefined") === None)
    assert(fromString("blah") === None)
    
    
    assert(fromString("SUCCEEDED") === Some(Succeeded))
    assert(fromString("SKIPPED") === Some(Skipped))
    assert(fromString("FAILED") === Some(Failed))
    assert(fromString("FAILEDWITHEXCEPTION") === Some(FailedWithException))
    assert(fromString("NOTSTARTED") === Some(NotStarted))
    assert(fromString("SUBMITTED") === Some(Submitted))
    assert(fromString("RUNNING") === Some(Running))
    assert(fromString("TERMINATED") === Some(Terminated))
    assert(fromString("UNKNOWN") === Some(Unknown))
    assert(fromString("PERMANENTFAILURE") === Some(PermanentFailure))
    assert(fromString("   ") === None)
    assert(fromString("UNDEFINED") === None)
    assert(fromString("BLAH") === None)
    
    assert(fromString("SuCcEeDeD") === Some(Succeeded))
    assert(fromString("SkIpPeD") === Some(Skipped))
    assert(fromString("FaIlEd") === Some(Failed))
    assert(fromString("FaIlEdWiThExCePtIoN") === Some(FailedWithException))
    assert(fromString("NoTsTaRtEd") === Some(NotStarted))
    assert(fromString("SuBmItTeD") === Some(Submitted))
    assert(fromString("RuNnInG") === Some(Running))
    assert(fromString("TeRmInAtEd") === Some(Terminated))
    assert(fromString("UnKnOwN") === Some(Unknown))
    assert(fromString("PeRmAnEnTfAiLuRe") === Some(PermanentFailure))
    assert(fromString("UnDeFiNeD") === None)
    assert(fromString("bLaH") === None)
  }

  test("fromExitCode") {
    assert(fromExitCode(0) === Succeeded)
    assert(fromExitCode(1) === Failed)
    assert(fromExitCode(-1) === Failed)
    assert(fromExitCode(42) === Failed) // scalastyle:ignore magic.number
  }

}
