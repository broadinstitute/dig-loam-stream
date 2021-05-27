package loamstream.model.execute

import java.time.Instant
import java.time.LocalDateTime

import scala.concurrent.duration.Duration

import loamstream.drm.Queue
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory

/**
 * @author kyuksel
 * @author clint
 *         date: 3/9/17
 */
sealed trait Resources {
  def startTime: LocalDateTime

  def endTime: LocalDateTime
  
  def raw: Option[String] = None

  //TODO: Maybe java.time.Duration instead?
  final def elapsedTime: Duration = {
    import java.time.{ Duration => JDuration }
    import scala.concurrent.duration._
  
    JDuration.between(startTime, endTime).toMillis.milliseconds
  }
}

object Resources {
  final case class LocalResources(
      startTime: LocalDateTime,
      endTime: LocalDateTime) extends Resources
      
  final case class GoogleResources(
      clusterId: String,
      startTime: LocalDateTime,
      endTime: LocalDateTime) extends Resources
  
  object GoogleResources {
    def fromClusterAndLocalResources(clusterId: String, localResources: LocalResources): GoogleResources = {
      GoogleResources(clusterId, localResources.startTime, localResources.endTime)
    }
  }
  
  final case class AwsResources(
      clusterId: String,
      startTime: LocalDateTime,
      endTime: LocalDateTime) extends Resources
  
  trait DrmResources extends Resources {
    def memory: Memory
    def cpuTime: CpuTime
    def node: Option[String]
    def queue: Option[Queue]
    
    final def withNode(newNode: String): DrmResources = withNode(Option(newNode))
    
    def withNode(newNodeOpt: Option[String]): DrmResources
    
    final def withQueue(newQueue: Queue): DrmResources = withQueue(Option(newQueue))
    
    def withQueue(newQueueOpt: Option[Queue]): DrmResources
    
    override def toString: String = {
      s"${getClass.getSimpleName}(${memory},${cpuTime},${node},${queue},${startTime},${endTime},...)"
    }
  }
  
  object DrmResources {
    type ResourcesMaker[R <: DrmResources] = 
      (Memory, CpuTime, Option[String], Option[Queue], LocalDateTime, LocalDateTime, Option[String]) => R
    
    type FieldsTuple = (Memory, CpuTime, Option[String], Option[Queue], LocalDateTime, LocalDateTime, Option[String])
    
    def unapply(r: Resources): Option[FieldsTuple] = r match {
      case u: UgerResources => UgerResources.unapply(u)
      case l: LsfResources => LsfResources.unapply(l)
      case _ => None
    }
  }
  
  final case class UgerResources(
      memory: Memory,
      cpuTime: CpuTime,
      node: Option[String],
      queue: Option[Queue],
      startTime: LocalDateTime,
      endTime: LocalDateTime,
      override val raw: Option[String] = None) extends DrmResources {
    
    override def withNode(newNodeOpt: Option[String]): DrmResources = copy(node = newNodeOpt)
    
    override def withQueue(newQueueOpt: Option[Queue]): DrmResources = copy(queue = newQueueOpt)
  }
  
  final case class LsfResources(
      memory: Memory,
      cpuTime: CpuTime,
      node: Option[String],
      queue: Option[Queue],
      startTime: LocalDateTime,
      endTime: LocalDateTime,
      override val raw: Option[String] = None) extends DrmResources {
    
    override def withNode(newNodeOpt: Option[String]): DrmResources = copy(node = newNodeOpt)
    
    override def withQueue(newQueueOpt: Option[Queue]): DrmResources = copy(queue = newQueueOpt)
  }
  
  final case class SlurmResources(
      memory: Memory,
      cpuTime: CpuTime,
      node: Option[String],
      queue: Option[Queue],
      startTime: LocalDateTime,
      endTime: LocalDateTime,
      override val raw: Option[String] = None) extends DrmResources {
    
    override def withNode(newNodeOpt: Option[String]): DrmResources = copy(node = newNodeOpt)
    
    override def withQueue(newQueueOpt: Option[Queue]): DrmResources = copy(queue = newQueueOpt)
  }
}
