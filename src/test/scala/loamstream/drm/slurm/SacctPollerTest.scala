package loamstream.drm.slurm

import org.scalatest.FunSuite
import scala.util.Success
import loamstream.drm.DrmTaskId
import loamstream.drm.DrmStatus

final class SacctPollerTest extends FunSuite {
  test("parseDataLine - good input") {
    val line = "71|COMPLETED|0:0"
    
    val actual = SacctPoller.parseDataLine(line)
    
    val expected = Success(DrmTaskId("71", 0) -> DrmStatus.CommandResult(0))
    
    assert(actual === expected)
  }
}