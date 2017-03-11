package loamstream.oracle

import java.time.Instant
import scala.concurrent.duration.Duration
import scala.util.Try
import loamstream.util.Tries
import loamstream.util.Options
import loamstream.oracle.uger.Queue

/**
 * @author clint
 * Mar 8, 2017
 */
sealed trait Resources

object Resources {
  //TODO
  case object LocalResources extends Resources 
  //TODO
  case object GoogleResources extends Resources
  
  final case class UgerResources(
      memory: Memory,
      cpuTime: CpuTime,
      node: Option[String],
      queue: Option[Queue],
      startTime: Instant,
      endTime: Instant) extends Resources {
    
    def runTime: Duration = {
      import java.time.{ Duration => JDuration }
      import scala.concurrent.duration._
      
      JDuration.between(startTime, endTime).toMillis.milliseconds
    }
    
    def withNode(newNode: String): UgerResources = copy(node = Option(newNode))
    
    def withQueue(newQueue: Queue): UgerResources = copy(queue = Option(newQueue))
  }
  
  object UgerResources {
    object Keys {
      val cpu = "cpu"
      val mem = "mem"
      val startTime = "start_time"
      val endTime = "end_time"
    }
    
    /**
     * Parse an untyped map, like the one returned by org.ggf.drmaa.JobInfo.getResourceUsage()
     */
    def fromMap(m: Map[Any, Any]): Try[UgerResources] = {
      def tryGet[A](key: String)(convert: String => A): Try[A] = {
        import Options.toTry
        
        for {
          valueMaybeNull <- toTry(m.get(key))(s"Resource usage field '$key' not found")
          value <- toTry(Option(valueMaybeNull))(s"Null value for field '$key'")
        } yield {
          convert(value.toString.trim)
        }
      }
      
      import scala.concurrent.duration._
      
      //NB: Uger reports cpu time as a floating-point number of cpu-seconds. 
      def toCpuTime(s: String) = CpuTime(s.toDouble.seconds)
      //NB: Uger reports memory used as a floating-point number of gigabytes.
      def toMemory(s: String) = Memory.inGb(s.toDouble)
      //NB: Uger returns times as floating-point numbers of milliseconds since the epoch, but the values
      //always represent integers, like '1488840586288.0000'. (Note the trailing .0000)
      //So we convert to a double first, then to a long, to drop the superfluous fractional part.
      //TODO: Maybe use a regex here to drop the '.nnnn' bit, if this 2-step conversion loses data.
      def toInstant(s: String) = Instant.ofEpochMilli(s.toDouble.toLong)
      
      //NB: JobInfo.getResourceUsage, the method one will most likely call to get a map to pass to
      //this method, can return null.  :( 
      if(m == null) { Tries.failure(s"Null map passed in") } //scalastyle:ignore null
      else {
        for {
          cpu <- tryGet(Keys.cpu)(toCpuTime)
          mem <- tryGet(Keys.mem)(toMemory)
          startTime <- tryGet(Keys.startTime)(toInstant)
          endTime <- tryGet(Keys.endTime)(toInstant)
        } yield {
          //TODO: Get node somehow (qacct?)
          //TODO: Get queue somehow (pass it in, qacct?)
          UgerResources(mem, cpu, node = None, queue = None, startTime, endTime)
        }
      }
    }
  }
}
