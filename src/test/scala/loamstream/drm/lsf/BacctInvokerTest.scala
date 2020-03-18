package loamstream.drm.lsf

import org.scalatest.FunSuite

import loamstream.drm.DrmTaskId

/**
 * @author clint
 * Apr 25, 2019
 */
final class BacctInvokerTest extends FunSuite {
  test("makeTokens") {
    assert(BacctInvoker.makeTokens("foo", DrmTaskId("bar", 42)) === Seq("foo", "-l", "bar"))
  }
}
