package loamstream.uger

import org.ggf.drmaa2.JobState

/**
 * @author clint
 * date: Jun 16, 2016
 */
sealed trait JobStatus {
  import JobStatus._
  
  def isDone: Boolean = this == Done
  def isFailed: Boolean = this == Failed
  def isQueued: Boolean = this == Queued
  def isQueuedHeld: Boolean = this == QueuedHeld
  def isRequeued: Boolean = this == Requeued
  def isRequeuedHeld: Boolean = this == RequeuedHeld
  def isRunning: Boolean = this == Running
  def isSuspended: Boolean = this == Suspended
  def isUndetermined: Boolean = this == Undetermined
}

object JobStatus {
  case object Done extends JobStatus
  case object Failed extends JobStatus
  case object Queued extends JobStatus
  case object QueuedHeld extends JobStatus
  case object Requeued extends JobStatus
  case object RequeuedHeld extends JobStatus
  case object Running extends JobStatus
  case object Suspended extends JobStatus
  case object Undetermined extends JobStatus
  
  def fromJobState(state: JobState): JobStatus = {
    require(state != null)
    
    import JobState._
    
    state match {
      case DONE => Done
      case FAILED => Failed
      case QUEUED => Queued
      case QUEUED_HELD => QueuedHeld
      case REQUEUED => Requeued
      case REQUEUED_HELD => RequeuedHeld
      case RUNNING => Running
      case SUSPENDED => Suspended
      case UNDETERMINED => Undetermined
    }
  }
}