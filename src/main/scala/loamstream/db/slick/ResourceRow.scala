package loamstream.db.slick

import java.sql.Timestamp

import loamstream.model.execute.Resources
import loamstream.model.execute.Resources.{GoogleResources, LocalResources, UgerResources}
import loamstream.uger.Queue
import loamstream.model.execute.Memory
import loamstream.model.execute.CpuTime

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
      LocalResourceRow(executionId, Timestamp.from(startTime), Timestamp.from(endTime))
    }
    case UgerResources(mem, cpu, node, queue, startTime, endTime) => {
      //TODO
      UgerResourceRow(executionId, mem.gb, cpu.seconds, node, queue.map(_.name),
        Timestamp.from(startTime), Timestamp.from(endTime))
    }
    case GoogleResources(cluster, startTime, endTime) => {
      //TODO
      GoogleResourceRow(executionId, cluster, Timestamp.from(startTime), Timestamp.from(endTime))
    }
  }
}

final case class LocalResourceRow(executionId: Int,
                                  startTime: Timestamp,
                                  endTime: Timestamp) extends ResourceRow {
  
  override def toResources: Resources = LocalResources(startTime.toInstant, endTime.toInstant)
}

final case class UgerResourceRow(executionId: Int,
                                 mem: Double,
                                 cpu: Double,
                                 node: Option[String],
                                 queue: Option[String],
                                 startTime: Timestamp,
                                 endTime: Timestamp) extends ResourceRow {

  override def toResources: Resources = {
    import scala.concurrent.duration._
    
    UgerResources(
        Memory.inGb(mem), CpuTime(cpu.seconds), node, 
        queue.flatMap(Queue.fromString), startTime.toInstant, endTime.toInstant)
  }
}

final case class GoogleResourceRow(executionId: Int,
                                   cluster: String,
                                   startTime: Timestamp,
                                   endTime: Timestamp) extends ResourceRow {

  override def toResources: Resources = GoogleResources(cluster, startTime.toInstant, endTime.toInstant)
}
