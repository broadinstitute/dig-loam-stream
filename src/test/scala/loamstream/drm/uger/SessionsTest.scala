package loamstream.drm.uger

import org.scalatest.FunSuite
import loamstream.util.Users


/**
 * @author clint
 * Jul 24, 2020
 */
final class SessionsTest extends FunSuite {
  test("sessionId") {
    val sid = Sessions.sessionId
    
    val prefix = s"LoamStream-${Users.currentUser}-"
    
    assert(sid.startsWith(prefix))
    
    //The size of a UUID
    assert(sid.size - prefix.size === 36)
  }
}
