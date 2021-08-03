package loamstream.drm.slurm

import org.scalatest.FunSuite
import scala.util.Success
import loamstream.drm.DrmTaskId
import loamstream.drm.DrmStatus

final class SqueuePollerTest extends FunSuite {
  test("parseDataLine - good input") {
    val line = "71_2|COMPLETED"
    
    val actual = SqueuePoller.parseDataLine(line)
    
    val expected = Success(Seq(DrmTaskId("71", 2) -> DrmStatus.CommandResult(0)))
    
    assert(actual === expected)
  }

  test("parseDataLine - good range input") {
    val line = "42_[1-3]|PD"
    
    val actual = SqueuePoller.parseDataLine(line)
    
    val expected = Success(Seq(
      DrmTaskId("42", 1) -> DrmStatus.Queued,
      DrmTaskId("42", 2) -> DrmStatus.Queued,
      DrmTaskId("42", 3) -> DrmStatus.Queued))
    
    assert(actual === expected)
  }
}