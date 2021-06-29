package loamstream.drm.slurm

import org.scalatest.FunSuite
import scala.util.Success
import loamstream.drm.DrmTaskId
import loamstream.drm.DrmStatus

final class SqueuePollerTest extends FunSuite {
  test("parseDataLine - good input") {
    val line = "71_2|COMPLETED"
    
    val actual = SqueuePoller.parseDataLine(line)
    
    val expected = Success(DrmTaskId("71", 2) -> DrmStatus.CommandResult(0))
    
    assert(actual === expected)
  }
}