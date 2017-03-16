package loamstream.model.execute

import java.time.Instant

import loamstream.uger.Queue

/**
 * @author kyuksel
 *         date: 3/9/17
 */
sealed trait Resources {
  def startTime: Option[Instant]

  def endTime: Option[Instant]

  def elapsedTime: Option[Instant] = for {
    st <- startTime
    et <- endTime
  } yield et.minusMillis(st.toEpochMilli)
}

final case class LocalResources(startTime: Option[Instant],
                                endTime: Option[Instant]) extends Resources

final case class UgerResources(mem: Option[Float],
                               cpu: Option[Float],
                               node: Option[String],
                               queue: Option[Queue],
                               startTime: Option[Instant],
                               endTime: Option[Instant]) extends Resources

final case class GoogleResources(cluster: Option[String],
                                 startTime: Option[Instant],
                                 endTime: Option[Instant]) extends Resources
