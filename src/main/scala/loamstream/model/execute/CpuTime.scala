package loamstream.model.execute

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit


/**
 * @author clint
 * Mar 7, 2017
 */
final case class CpuTime(duration: Duration) {
  def seconds: Double = duration.toUnit(TimeUnit.SECONDS)
  
  //NB: override toString since Duration's toString uses different units at different times,
  //and I'm not sure why.
  override def toString: String = s"${getClass.getSimpleName}($seconds seconds)"
}

object CpuTime {
  def inSeconds(secs: Double): CpuTime = {
    require(secs >= 0.0)
    
    import scala.concurrent.duration._
    
    CpuTime(secs.seconds)
  }
}
