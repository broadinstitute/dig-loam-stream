package loamstream.drm.uger

import org.scalatest.FunSuite
import loamstream.drm.SessionSource


/**
 * @author clint
 * Jul 24, 2020
 */
final class QstatTest extends FunSuite {
  test("makeTokens") {
    import Qstat.makeTokens
    
    val sessionSource = MockSessionSource("lalala")
    
    assert(makeTokens("foo", sessionSource) === Seq("foo", "-si", "lalala"))
  }
}
