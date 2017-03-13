package loamstream.oracle

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit


/**
 * @author clint
 * Mar 7, 2017
 */
final case class CpuTime(duration: Duration) {
  def seconds: Double = duration.toUnit(TimeUnit.SECONDS)
}
