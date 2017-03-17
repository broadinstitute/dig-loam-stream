package loamstream.model.execute

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit


/**
 * @author clint
 * Mar 7, 2017
 */
final case class CpuTime(duration: Duration) {
  def seconds: Double = duration.toUnit(TimeUnit.SECONDS)
}

object CpuTime {
  def inSeconds(secs: Double): CpuTime = {
    require(secs >= 0.0)
    
    import scala.concurrent.duration._
    
    CpuTime(secs.seconds)
  }
}
