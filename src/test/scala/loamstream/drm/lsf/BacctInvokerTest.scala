package loamstream.drm.lsf

import org.scalatest.FunSuite

/**
 * @author clint
 * Apr 25, 2019
 */
final class BacctInvokerTest extends FunSuite {
  test("makeTokens") {
    assert(BacctInvoker.makeTokens("foo", "bar") === Seq("foo", "-l", "bar"))
  }
}
