package loamstream.model.quantities

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationDouble


/**
 * @author clint
 * Mar 7, 2017
 */
final case class CpuTime(duration: Duration) {
  def seconds: Double = duration.toUnit(TimeUnit.SECONDS)
  
  def hours: Double = duration.toUnit(TimeUnit.HOURS)
  
  //NB: override toString since Duration's toString uses different units at different times
  override def toString: String = s"${getClass.getSimpleName}($seconds seconds)"
}

object CpuTime {
  import scala.concurrent.duration.DurationDouble
  
  def inSeconds(secs: Double): CpuTime = in(secs, _.seconds)
  
  def inHours(hours: Double): CpuTime = in(hours, _.hours)
  
  private def in(howMany: Double, toDuration: Double => Duration): CpuTime = {
    require(howMany >= 0.0)
    
    CpuTime(toDuration(howMany))
  }
}
