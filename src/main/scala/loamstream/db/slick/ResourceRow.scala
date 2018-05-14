package loamstream.db.slick

import java.sql.Timestamp

import loamstream.model.execute.Resources
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.execute.Resources.LsfResources
import loamstream.drm.Queue
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import java.time.Instant

/**
 * @author kyuksel
 *         date: 3/10/17
 */
sealed trait ResourceRow {
  def toResources: Resources
  
  //NB: Double-dispatch pattern, to avoid repeated pattern-matches in SlickLoamDao.
  def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int]
}

object ResourceRow {
  def fromResources(resources: Resources, executionId: Int): ResourceRow = resources match {
    case LocalResources(startTime, endTime) => {
      LocalResourceRow(executionId, Timestamp.from(startTime), Timestamp.from(endTime))
    }
    case UgerResources(mem, cpu, node, queue, startTime, endTime) => {
      UgerResourceRow(executionId, mem.kb, cpu.seconds, node, queue.map(_.name),
        Timestamp.from(startTime), Timestamp.from(endTime))
    }
    case LsfResources(mem, cpu, node, queue, startTime, endTime) => {
      LsfResourceRow(executionId, mem.kb, cpu.seconds, node, queue.map(_.name),
        Timestamp.from(startTime), Timestamp.from(endTime))
    }
    case GoogleResources(cluster, startTime, endTime) => {
      GoogleResourceRow(executionId, cluster, Timestamp.from(startTime), Timestamp.from(endTime))
    }
    case _ => sys.error(s"Unexpected resource type: $resources")
  }
}

final case class LocalResourceRow(executionId: Int,
                                  startTime: Timestamp,
                                  endTime: Timestamp) extends ResourceRow {
  
  override def toResources: Resources = LocalResources(startTime.toInstant, endTime.toInstant)
  
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.localResources.insertOrUpdate(this)
  }
}

private[slick] abstract class DrmResourceRow[R <: Resources](
    makeResources: (Memory, CpuTime, Option[String], Option[Queue], Instant, Instant) => R) extends ResourceRow {
  
  def executionId: Int
  def mem: Double
  def cpu: Double
  def node: Option[String]
  def queue: Option[String]
  def startTime: Timestamp
  def endTime: Timestamp
  
  override def toResources: Resources = {
    import scala.concurrent.duration._
    
    makeResources(
        Memory.inKb(mem), 
        CpuTime(cpu.seconds), 
        node, 
        queue.map(Queue(_)), 
        startTime.toInstant, 
        endTime.toInstant)
  }
}

final case class UgerResourceRow(
    executionId: Int,
    mem: Double,
    cpu: Double,
    node: Option[String],
    queue: Option[String],
    startTime: Timestamp,
    endTime: Timestamp) extends DrmResourceRow[UgerResources](UgerResources.apply) {

  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.ugerResources.insertOrUpdate(this)
  }
}

final case class LsfResourceRow(
    executionId: Int,
    mem: Double,
    cpu: Double,
    node: Option[String],
    queue: Option[String],
    startTime: Timestamp,
    endTime: Timestamp) extends DrmResourceRow[LsfResources](LsfResources.apply) {

  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.lsfResources.insertOrUpdate(this)
  }
}

final case class GoogleResourceRow(executionId: Int,
                                   cluster: String,
                                   startTime: Timestamp,
                                   endTime: Timestamp) extends ResourceRow {

  override def toResources: Resources = GoogleResources(cluster, startTime.toInstant, endTime.toInstant)
  
  override def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int] = {
    import tables.driver.api._
    
    tables.googleResources.insertOrUpdate(this)
  }
}
