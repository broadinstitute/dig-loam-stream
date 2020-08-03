package loamstream.drm.uger

import loamstream.drm.SessionSource


/**
 * @author clint
 * Jul 28, 2020
 */
final case class MockSessionSource(getSession: String, isInitialized: Boolean = false) extends SessionSource {
  override def stop(): Unit = ()
}
