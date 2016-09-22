package loamstream.uger

import loamstream.model.jobs.JobState
import org.ggf.drmaa.Session

/**
 * @author clint
 * date: Jun 16, 2016
 * 
 * An ADT/"Enum" to represent job statuses as reported by UGER.  Values roughly correspond to constants in 
 * org.ggf.drmaa.Session.
 */
sealed trait JobStatus {
  import JobStatus._
  
  def isDone: Boolean = this == Done
  def isFailed: Boolean = this == Failed
  def isQueued: Boolean = this == Queued
  def isQueuedHeld: Boolean = this == QueuedHeld
  def isRunning: Boolean = this == Running
  def isSuspended: Boolean = this == Suspended
  def isUndetermined: Boolean = this == Undetermined
  def isDoneUndetermined: Boolean = this == DoneUndetermined
  
  //TODO: Does Undetermined belong here?
  def notFinished: Boolean = isQueued || isQueuedHeld || isRunning || isSuspended || isUndetermined
  
  def isFinished: Boolean = !notFinished
}

object JobStatus {
  case object Done extends JobStatus
  case object DoneUndetermined extends JobStatus
  case object Failed extends JobStatus
  case object Queued extends JobStatus
  case object QueuedHeld extends JobStatus
  case object Requeued extends JobStatus
  case object RequeuedHeld extends JobStatus
  case object Running extends JobStatus
  case object Suspended extends JobStatus
  case object Undetermined extends JobStatus
  
  import Session._
  
  def fromUgerStatusCode(status: Int): JobStatus = status match {
    case QUEUED_ACTIVE                                              => Queued
    case SYSTEM_ON_HOLD | USER_ON_HOLD | USER_SYSTEM_ON_HOLD        => QueuedHeld
    case RUNNING                                                    => Running
    case SYSTEM_SUSPENDED | USER_SUSPENDED | USER_SYSTEM_SUSPENDED  => Suspended
    case DONE                                                       => Done
    case FAILED                                                     => Failed
    case UNDETERMINED | _                                           => Undetermined
  }

  def toJobState(status: JobStatus): JobState = status match {
    case Done                                                       => JobState.Succeeded
    case DoneUndetermined                                           => JobState.Failed
    case Failed                                                     => JobState.Failed
    case Queued | QueuedHeld | Requeued | RequeuedHeld              => JobState.Running
    case Running                                                    => JobState.Running
    case Suspended                                                  => JobState.Failed
    case Undetermined                                               => JobState.Failed
  }
}
