package loamstream.drm.uger

import org.scalatest.FunSuite
import scala.util.Success

/**
 * @author clint
 * Jul 27, 2020
 */
final class QconfTest extends FunSuite {
  test("makeCreateTokens") {
    assert(Qconf.makeCreateTokens("foo") === Seq("foo", "-csi"))
  }
  
  test("makeDeleteTokens") {
    assert(Qconf.makeDeleteTokens("foo", "bar") === Seq("foo", "-dsi", "bar"))
  }
  
  test("parseOutput") {
    assert(Qconf.parseOutput(Nil).isFailure)
    
    assert(Qconf.parseOutput(Seq(" foo  ", "bar", "baz")) === Success("foo"))
  }
}
