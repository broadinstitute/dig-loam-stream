package loamstream.drm.uger

import org.scalatest.FunSuite
import loamstream.drm.DrmTaskId
import loamstream.drm.DrmTaskArray
import loamstream.TestHelpers

/**
 * @author clint
 * Apr 25, 2019
 */
final class QacctInvokerTest extends FunSuite {
  test("makeTokens - DrmTaskId") {
    assert(QacctInvoker.makeTokens("foo", Left(DrmTaskId("bar", 42))) === Seq("foo", "-j", "bar", "-t", "42"))
  }
  
  test("makeTokens - DrmTaskArray") {
    val taskArray = DrmTaskArray(
        TestHelpers.config.ugerConfig.get,
        Nil,
        "some-job-name",
        "/dev/null",
        "/dev/null")
    
    assert(QacctInvoker.makeTokens("foo", Right(taskArray)) === Seq("foo", "-j", "some-job-name"))
  }
}
