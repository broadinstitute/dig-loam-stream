package loamstream.drm.uger

import org.scalatest.FunSuite


/**
 * @author clint
 * Jul 24, 2020
 */
final class QstatTest extends FunSuite {
  test("makeTokens") {
    import Qstat.makeTokens
    
    assert(makeTokens("foo") === Seq("foo", "-si", Sessions.sessionId))
  }
}
