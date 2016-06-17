package loamstream.uger

/**
 * @author clint
 * date: Jun 16, 2016
 */
sealed trait JobStatus {
  import JobStatus._
  
  def isUnknown: Boolean = this == Unknown
  def isRunning: Boolean = this == Running
  def isFinished: Boolean = this == Finished
}

object JobStatus {
  case object Unknown extends JobStatus  
  case object Running extends JobStatus
  case object Finished extends JobStatus
}