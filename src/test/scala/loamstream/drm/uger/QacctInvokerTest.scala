package loamstream.drm.uger

import org.scalatest.FunSuite
import loamstream.drm.DrmTaskId

/**
 * @author clint
 * Apr 25, 2019
 */
final class QacctInvokerTest extends FunSuite {
  test("makeTokens") {
    assert(QacctInvoker.makeTokens("foo", DrmTaskId("bar", 42)) === Seq("foo", "-j", "bar", "-t", "42"))
  }
}
