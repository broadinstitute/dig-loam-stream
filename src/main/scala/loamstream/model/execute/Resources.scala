package loamstream.model.execute

import java.time.Instant

import loamstream.uger.Queue

/**
 * @author kyuksel
 *         date: 3/9/17
 */
trait Resources {
  def mem: Option[Int] = None

  def cpu: Option[Int] = None

  def node: Option[String] = None

  def queue: Option[Queue] = None

  def startTime: Option[Instant] = None

  def endTime: Option[Instant] = None

  def elapsedTime: Option[Instant] = for {
    st: Instant <- startTime
    et: Instant <- endTime
  } yield et.minusMillis(st.toEpochMilli)
}
