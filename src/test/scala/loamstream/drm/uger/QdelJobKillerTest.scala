package loamstream.drm.uger

import org.scalatest.FunSuite
import loamstream.drm.SessionTracker
import loamstream.drm.DrmTaskId

/**
 * @author clint
 * Jan 11, 2021
 */
final class QdelJobKillerTest extends FunSuite {
  test("makeTokens") {
    val sessionTracker = new SessionTracker.Default
    
    def tokens = QdelJobKiller.makeTokens(sessionTracker, "foo", "bar")
    
    assert(sessionTracker.isEmpty)
    assert(tokens === Seq("foo", "-u", "bar"))
    
    sessionTracker.register(DrmTaskId("blarg", 42))
    
    assert(sessionTracker.nonEmpty)
    assert(tokens === Seq("foo", "-u", "bar", "blarg"))
    
    sessionTracker.register(DrmTaskId("blarg", 43))
    sessionTracker.register(DrmTaskId("blah", 1))
    
    assert(sessionTracker.nonEmpty)
    assert(tokens.startsWith(Seq("foo", "-u", "bar")))
    assert(tokens.size === 4)
    assert(tokens(3).split(",").toSet === Set("blarg", "blah"))
    
    sessionTracker.register(DrmTaskId("zuh", 2))
    
    assert(sessionTracker.nonEmpty)
    assert(sessionTracker.nonEmpty)
    assert(tokens.startsWith(Seq("foo", "-u", "bar")))
    assert(tokens.size === 4)
    assert(tokens(3).split(",").toSet === Set("blarg", "blah", "zuh"))
  }
}
