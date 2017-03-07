package loamstream.model.execute

import java.time.Instant

import loamstream.uger.Queue

/**
 * @author kyuksel
 *         date: 3/7/17
 */
trait Settings {
  def mem: Option[Float] = None
  def cpu: Option[Float] = None
  def startTime: Option[Instant] = None
  def endTime: Option[Instant] = None
  def node: Option[String] = None
  def queue: Option[Queue] = None

  def elapsedTime: Option[Instant] = for {
    st: Instant <- startTime
    et: Instant <- endTime
  } yield et.minusMillis(st.toEpochMilli)
}
