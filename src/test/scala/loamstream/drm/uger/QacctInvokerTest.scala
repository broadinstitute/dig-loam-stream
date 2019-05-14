package loamstream.drm.uger

import org.scalatest.FunSuite

/**
 * @author clint
 * Apr 25, 2019
 */
final class QacctInvokerTest extends FunSuite {
  test("makeTokens") {
    assert(QacctInvoker.makeTokens("foo", "bar") === Seq("foo", "-j", "bar"))
  }
}
