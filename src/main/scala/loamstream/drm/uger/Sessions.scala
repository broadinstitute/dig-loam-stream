package loamstream.drm.uger

import loamstream.util.Users
import java.util.UUID

/**
 * @author clint
 * Jul 15, 2020
 */
object Sessions {
  val sessionId: String = s"LoamStream-${Users.currentUser}-${UUID.randomUUID}"
}
