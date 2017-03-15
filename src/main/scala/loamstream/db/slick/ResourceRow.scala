package loamstream.db.slick

import java.sql.Timestamp

import loamstream.model.execute.{GoogleResources, LocalResources, Resources, UgerResources}
import loamstream.uger.Queue

/**
 * @author kyuksel
 *         date: 3/10/17
 */
sealed trait ResourceRow {
  def toResources: Resources
}

final case class LocalResourceRow(executionId: Int,
                                  startTime: Option[Timestamp],
                                  endTime: Option[Timestamp]) {
  def toResources = LocalResources(startTime.map(_.toInstant), endTime.map(_.toInstant))
}

final case class UgerResourceRow(executionId: Int,
                                 mem: Option[Float],
                                 cpu: Option[Float],
                                 node: Option[String],
                                 queue: Option[String],
                                 startTime: Option[Timestamp],
                                 endTime: Option[Timestamp]) {

  def toResources: Resources = UgerResources(mem, cpu, node, queue.map(Queue.fromString),
    startTime.map(_.toInstant), endTime.map(_.toInstant))
}

final case class GoogleResourceRow(executionId: Int,
                                   cluster: Option[String],
                                   startTime: Option[Timestamp],
                                   endTime: Option[Timestamp]) {

  def toResources: Resources = GoogleResources(cluster, startTime.map(_.toInstant), endTime.map(_.toInstant))
}
