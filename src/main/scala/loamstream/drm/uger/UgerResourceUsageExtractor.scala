package loamstream.drm.uger

import java.time.Instant

import scala.concurrent.duration.DurationDouble
import scala.util.Failure
import scala.util.Try

import org.ggf.drmaa.JobInfo

import loamstream.drm.ResourceUsageExtractor
import loamstream.model.execute.Resources.DrmResources
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import loamstream.util.Tries


/**
 * @author clint
 * May 11, 2018
 */
object UgerResourceUsageExtractor extends ResourceUsageExtractor[JobInfo] {
  override def toResources(jobInfo: JobInfo): Try[DrmResources] = {
    import scala.collection.JavaConverters._

    fromUgerMap(jobInfo.getResourceUsage.asScala.toMap)
  }
  
  private[uger] object UgerKeys {
    val cpu = "cpu"
    val mem = "ru_maxrss"
    val startTime = "start_time"
    val endTime = "end_time"
  }
  
  /**
   * Parse an untyped map, like the one returned by org.ggf.drmaa.JobInfo.getResourceUsage()
   */
  private[uger] def fromUgerMap(m: Map[Any, Any]): Try[UgerResources] = {
    def tryGet[A](key: String): Try[String] = {
      import loamstream.util.Options.toTry
      
      for {
        valueMaybeNull <- toTry(m.get(key))(s"Resource usage field '$key' not found", UgerException)
        value <- toTry(Option(valueMaybeNull))(s"Null value for field '$key'", UgerException)
      } yield {
        value.toString.trim
      }
    }
    
    import scala.concurrent.duration._
    
    //NB: Uger reports cpu time as a floating-point number of cpu-seconds. 
    def toCpuTime(s: String) = CpuTime(s.toDouble.seconds)

    //NB: The value of qacct's ru_maxrss field (in kilobytes) is the closest approximation of
    //a Uger job's memory utilization
    def toMemory(s: String) = Memory.inKb(s.toDouble)

    //NB: Uger returns times as floating-point numbers of milliseconds since the epoch, but the values
    //always represent integers, like '1488840586288.0000'. (Note the trailing .0000)
    //So we convert to a double first, then to a long, to drop the superfluous fractional part.
    //TODO: Maybe use a regex here to drop the '.nnnn' bit, if this 2-step conversion loses data.
    def toInstant(s: String) = Instant.ofEpochMilli(s.toDouble.toLong)
    
    //NB: JobInfo.getResourceUsage, the method one will most likely call to get a map to pass to
    //this method, can return null.  :( 
    if(m == null) { Tries.failure(s"Null map passed in", UgerException) } //scalastyle:ignore null
    else {
      val attempt = for {
        cpu <- tryGet(UgerKeys.cpu).map(toCpuTime)
        mem <- tryGet(UgerKeys.mem).map(toMemory)
        startTime <- tryGet(UgerKeys.startTime).map(toInstant)
        endTime <- tryGet(UgerKeys.endTime).map(toInstant)
      } yield {
        //NB: Node and queue will be filled in later
        UgerResources(memory = mem, cpuTime = cpu, node = None, queue = None, startTime = startTime, endTime = endTime)
      }
      
      //Wrap any exceptions in UgerException, so we can check for that in Drmaa1Client
      attempt.recoverWith {
        case ue: UgerException => attempt
        case e: Exception => Failure(new UgerException(e))
      }
    }
  }
}
