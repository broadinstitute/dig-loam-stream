package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.TestHelpers

/**
 * @author kyuksel
 *         date: 3/29/17
 */
final class JobStatusTest extends FunSuite {
  import JobStatus._
  
  test("canStopExecution") {
    assert(Succeeded.canStopExecution === false)
    assert(Skipped.canStopExecution === false)
    assert(Failed.canStopExecution === true)
    assert(FailedWithException.canStopExecution === true)
    assert(NotStarted.canStopExecution === false)
    assert(Submitted.canStopExecution === false)
    assert(Running.canStopExecution === false)
    assert(Terminated.canStopExecution === true)
    assert(Unknown.canStopExecution === false)
    assert(CouldNotStart.canStopExecution === true)
    assert(FailedPermanently.canStopExecution === true)
    assert(WaitingForOutputs.canStopExecution === false)
  }
  
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
    assert(CouldNotStart.isPermanentFailure === false)
    assert(FailedPermanently.isPermanentFailure === true)
    assert(WaitingForOutputs.isPermanentFailure === false)
  }
  
  test("isCouldNotStart") {
    assert(Succeeded.isCouldNotStart === false)
    assert(Skipped.isCouldNotStart === false)
    assert(Failed.isCouldNotStart === false)
    assert(FailedWithException.isCouldNotStart === false)
    assert(NotStarted.isCouldNotStart === false)
    assert(Submitted.isCouldNotStart === false)
    assert(Running.isCouldNotStart === false)
    assert(Terminated.isCouldNotStart === false)
    assert(Unknown.isCouldNotStart === false)
    assert(CouldNotStart.isCouldNotStart === true)
    assert(FailedPermanently.isCouldNotStart === false)
    assert(WaitingForOutputs.isCouldNotStart === false)
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
    assert(CouldNotStart.isSuccess === false)
    assert(FailedPermanently.isSuccess === false)
    assert(WaitingForOutputs.isSuccess === true)
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
    assert(CouldNotStart.isSkipped === false)
    assert(FailedPermanently.isSkipped === false)
    assert(WaitingForOutputs.isSkipped === false)
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
    assert(CouldNotStart.isFailure === false)
    assert(FailedPermanently.isFailure === true)
    assert(WaitingForOutputs.isFailure === false)
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
    assert(CouldNotStart.isFinished === true)
    assert(FailedPermanently.isFinished === true)
    assert(WaitingForOutputs.isFinished === true)
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
    assert(CouldNotStart.notFinished === false)
    assert(FailedPermanently.notFinished === false)
    assert(WaitingForOutputs.notFinished === false)
  }
  
  test("fromString") {
    import TestHelpers.to1337Speak
    
    def doTest(baseString: String, expected: Option[JobStatus]): Unit = {
      def actuallyDoTest(s: String): Unit = assert(fromString(s) === expected)

      actuallyDoTest(baseString)
      actuallyDoTest(baseString.toUpperCase)
      actuallyDoTest(baseString.toLowerCase)
      actuallyDoTest(to1337Speak(baseString))
    }
    
    doTest("Succeeded", Some(Succeeded))
    doTest("Skipped", Some(Skipped))
    doTest("Failed", Some(Failed))
    doTest("FailedWithException", Some(FailedWithException))
    doTest("NotStarted", Some(NotStarted))
    doTest("Submitted", Some(Submitted))
    doTest("Running", Some(Running))
    doTest("Terminated", Some(Terminated))
    doTest("Unknown", Some(Unknown))
    doTest("PermanentFailure", Some(FailedPermanently))
    doTest("CouldNotStart", Some(CouldNotStart))
    doTest("WaitingForOutputs", Some(WaitingForOutputs))
    doTest("", None)
    doTest("Undefined", None)
    doTest("blah", None)
  }

  test("fromExitCode") {
    assert(fromExitCode(0) === WaitingForOutputs)
    assert(fromExitCode(1) === Failed)
    assert(fromExitCode(-1) === Failed)
    assert(fromExitCode(42) === Failed)
  }

  test("values") {
    val expected = Set(
        Succeeded,
        Skipped,
        Failed,
        FailedWithException,
        NotStarted,
        Submitted,
        Running,
        Terminated,
        Unknown,
        CouldNotStart,
        FailedPermanently,
        WaitingForOutputs)
        
    assert(JobStatus.values === expected)
  }
}
