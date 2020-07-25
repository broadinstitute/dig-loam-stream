package loamstream.drm.uger

import loamstream.util.Users
import java.util.UUID
import loamstream.util.ValueBox
import loamstream.util.OneTimeLatch

/**
 * @author clint
 * Jul 15, 2020
 */
object Sessions {
  private val sessionIdBox: ValueBox[String] = ValueBox("")
  
  private val latch = new OneTimeLatch
  
  private[drm] def init(sid: String): Unit = latch.doOnce {
    sessionIdBox := sid
  }
  
  def sessionId: String = sessionIdBox.value
}
