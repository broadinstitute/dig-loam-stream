package loamstream.model.execute

import java.time.Instant

import scala.concurrent.duration.Duration
import loamstream.util.Options
import scala.util.Try
import loamstream.uger.UgerException
import scala.util.Failure
import loamstream.util.Tries
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.drm.Queue

/**
 * @author kyuksel
 * @author clint
 *         date: 3/9/17
 */
sealed trait Resources {
  def startTime: Instant

  def endTime: Instant

  //TODO: Maybe java.time.Duration instead?
  final def elapsedTime: Duration = {
    import java.time.{ Duration => JDuration }
    import scala.concurrent.duration._
  
    JDuration.between(startTime, endTime).toMillis.milliseconds
  }
}

object Resources {
  final case class LocalResources(
      startTime: Instant,
      endTime: Instant) extends Resources
      
  final case class GoogleResources(
      cluster: String,
      startTime: Instant,
      endTime: Instant) extends Resources
  
  object GoogleResources {
    def fromClusterAndLocalResources(cluster: String, localResources: LocalResources): GoogleResources = {
      GoogleResources(cluster, localResources.startTime, localResources.endTime)
    }
  }
  
  final case class DrmResources(
      memory: Memory,
      cpuTime: CpuTime,
      node: Option[String],
      queue: Option[Queue],
      startTime: Instant,
      endTime: Instant) extends Resources {
    
    def withNode(newNode: String): DrmResources = copy(node = Option(newNode))
    
    def withQueue(newQueue: Queue): DrmResources = copy(queue = Option(newQueue))
  }
}
