package loamstream.model.execute

import java.time.Instant

import loamstream.uger.Queue
import scala.concurrent.duration.Duration
import loamstream.util.Options
import scala.util.Try
import loamstream.uger.UgerException
import scala.util.Failure
import loamstream.util.Tries

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
      
  object LocalResources {
    //TODO: remove
    @deprecated("", "")
    def DUMMY: LocalResources = {
      val now = Instant.now
      
      LocalResources(now, now)
    }
  }
  
  final case class GoogleResources(
      cluster: String,
      startTime: Instant,
      endTime: Instant) extends Resources
  
  object GoogleResources {
    def fromClusterAndLocalResources(cluster: String, localResources: LocalResources): GoogleResources = {
      GoogleResources(cluster, localResources.startTime, localResources.endTime)
    }
  }
  
  final case class UgerResources(
      memory: Memory,
      cpuTime: CpuTime,
      node: Option[String],
      queue: Option[Queue],
      startTime: Instant,
      endTime: Instant) extends Resources {
    
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
      def tryGet[A](key: String): Try[String] = {
        import Options.toTry
        
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
      //NB: Uger reports memory used as a floating-point number of gigabytes.
      def toMemory(s: String) = Memory.inGb(s.toDouble)
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
          cpu <- tryGet(Keys.cpu).map(toCpuTime)
          mem <- tryGet(Keys.mem).map(toMemory)
          startTime <- tryGet(Keys.startTime).map(toInstant)
          endTime <- tryGet(Keys.endTime).map(toInstant)
        } yield {
          //TODO: Get node somehow (qacct?)
          //TODO: Get queue somehow (pass it in, qacct?)
          UgerResources(mem, cpu, node = None, queue = None, startTime, endTime)
        }
        
        //Wrap any exceptions in UgerException, so we can check for that in Drmaa1Client
        attempt.recoverWith {
          case ue: UgerException => attempt
          case e: Exception => Failure(new UgerException(e))
        }
      }
    }
  }
}
