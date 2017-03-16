package loamstream.db.slick

import java.sql.Timestamp

import loamstream.model.execute.Resources
import loamstream.model.execute.Resources.{GoogleResources, LocalResources, UgerResources}
import loamstream.uger.Queue

/**
 * @author kyuksel
 *         date: 3/10/17
 */
sealed trait ResourceRow {
  def toResources: Resources
}

object ResourceRow {
  def fromResources(resources: Resources, executionId: Int): ResourceRow = resources match {
    //TODO
    case LocalResources(startTime, endTime) => {
      LocalResourceRow(executionId, Option(startTime).map(Timestamp.from), Option(endTime).map(Timestamp.from))
    }
    case UgerResources(mem, cpu, node, queue, startTime, endTime) => {
      //TODO
      UgerResourceRow(executionId, mem, cpu, node, queue.map(_.name),
        Option(startTime).map(Timestamp.from), Option(endTime).map(Timestamp.from))
    }
    case GoogleResources(cluster, startTime, endTime) => {
      //TODO
      GoogleResourceRow(executionId, cluster, Option(startTime).map(Timestamp.from), Option(endTime).map(Timestamp.from))
    }
  }
}

final case class LocalResourceRow(executionId: Int,
                                  startTime: Option[Timestamp],
                                  endTime: Option[Timestamp]) extends ResourceRow {
  
  override def toResources: Resources = {
    LocalResources(startTime.map(_.toInstant), endTime.map(_.toInstant))
  }
}

final case class UgerResourceRow(executionId: Int,
                                 mem: Option[Float],
                                 cpu: Option[Float],
                                 node: Option[String],
                                 queue: Option[String],
                                 startTime: Option[Timestamp],
                                 endTime: Option[Timestamp]) extends ResourceRow {

  override def toResources: Resources = UgerResources(mem, cpu, node, queue.map(Queue.fromString),
    startTime.map(_.toInstant), endTime.map(_.toInstant))
}

final case class GoogleResourceRow(executionId: Int,
                                   cluster: Option[String],
                                   startTime: Option[Timestamp],
                                   endTime: Option[Timestamp]) extends ResourceRow {

  override def toResources: Resources = GoogleResources(cluster, startTime.map(_.toInstant), endTime.map(_.toInstant))
}
