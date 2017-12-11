package loamstream.model.jobs.log

import org.scalatest.FunSuite
import loamstream.model.jobs.JobStatus

/**
 * @author clint
 * Oct 2, 2017
 */
final class JobLogTest extends FunSuite {
  test("computePadding") {
    import JobLog.computePadding
    import JobStatus._
    
    assert(computePadding(Terminated) === " " * 9)
    assert(computePadding(NotStarted) === " " * 9)
    assert(computePadding(FailedPermanently) === " " * 2)
    assert(computePadding(Submitted) === " " * 10)
    assert(computePadding(CouldNotStart) === " " * 6)
    assert(computePadding(Succeeded) === " " * 10)
    assert(computePadding(Running) === " " * 12)
    assert(computePadding(Unknown) === " " * 12)
    assert(computePadding(FailedWithException) === " " * 0)
    assert(computePadding(Skipped) === " " * 12)
    assert(computePadding(Failed) === " " * 13)
  }
}
