package loamstream.oracle

import scala.concurrent.duration.Duration


/**
 * @author clint
 * Mar 7, 2017
 */
final case class CpuTime(duration: Duration) {
  def seconds: Long = duration.toSeconds
}
