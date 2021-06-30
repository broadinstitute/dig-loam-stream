package loamstream.drm.uger

import org.scalatest.FunSuite
import loamstream.drm.SessionTracker
import loamstream.drm.DrmTaskId
import scala.collection.compat._

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
    
    sessionTracker.register(Seq(DrmTaskId("blarg", 42)))
    
    assert(sessionTracker.nonEmpty)
    assert(tokens === Seq("foo", "-u", "bar", "blarg"))
    
    sessionTracker.register(Seq(DrmTaskId("blarg", 43)))
    sessionTracker.register(Seq(DrmTaskId("blah", 1)))
    
    assert(sessionTracker.nonEmpty)
    assert(tokens.startsWith(Seq("foo", "-u", "bar")))
    assert(tokens.size === 4)
    assert(tokens(3).split(",").to(Set) === Set("blarg", "blah"))
    
    sessionTracker.register(Seq(DrmTaskId("zuh", 2)))
    
    assert(sessionTracker.nonEmpty)
    assert(sessionTracker.nonEmpty)
    assert(tokens.startsWith(Seq("foo", "-u", "bar")))
    assert(tokens.size === 4)
    assert(tokens(3).split(",").to(Set) === Set("blarg", "blah", "zuh"))
  }
}
