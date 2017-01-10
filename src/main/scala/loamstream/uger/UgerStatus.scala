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
sealed trait UgerStatus {
  import UgerStatus._

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

object UgerStatus {
  case object Done extends UgerStatus
  case object DoneUndetermined extends UgerStatus
  case object Failed extends UgerStatus
  case object Queued extends UgerStatus
  case object QueuedHeld extends UgerStatus
  case object Requeued extends UgerStatus
  case object RequeuedHeld extends UgerStatus
  case object Running extends UgerStatus
  case object Suspended extends UgerStatus
  case object Undetermined extends UgerStatus

  final case class CommandResult(exitStatus: Int) extends UgerStatus

  import Session._

  def fromUgerStatusCode(status: Int): UgerStatus = status match {
    case QUEUED_ACTIVE                                              => Queued
    case SYSTEM_ON_HOLD | USER_ON_HOLD | USER_SYSTEM_ON_HOLD        => QueuedHeld
    case RUNNING                                                    => Running
    case SYSTEM_SUSPENDED | USER_SUSPENDED | USER_SYSTEM_SUSPENDED  => Suspended
    case DONE                                                       => Done
    case FAILED                                                     => Failed
    case UNDETERMINED | _                                           => Undetermined
  }

  def toJobState(status: UgerStatus): JobState = status match {
    case Done                                                       => JobState.Succeeded
    case CommandResult(exitStatus)                                  => JobState.CommandResult(exitStatus)
    case DoneUndetermined                                           => JobState.Failed
    case Failed                                                     => JobState.Failed
    //TODO: Perhaps these should be something like JobState.NotStarted?
    case Queued | QueuedHeld | Requeued | RequeuedHeld              => JobState.Running
    case Running                                                    => JobState.Running
    //TODO: Is this right?
    case Suspended                                                  => JobState.Failed
    //TODO: Is this right?
    case Undetermined                                               => JobState.Failed
  }
}
