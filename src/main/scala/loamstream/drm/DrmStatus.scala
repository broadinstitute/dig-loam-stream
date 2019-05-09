package loamstream.drm

import loamstream.model.jobs.{JobResult, JobStatus}
import org.ggf.drmaa.Session
import loamstream.model.execute.Resources.DrmResources

/**
 * @author clint
 * date: Jun 16, 2016
 * 
 * An ADT/"Enum" to represent job statuses as reported by UGER and other DRM systems.  
 * Values roughly correspond to constants in org.ggf.drmaa.Session.
 */
sealed trait DrmStatus {
  import DrmStatus._

  def isDone: Boolean = this == Done
  def isFailed: Boolean = this == Failed
  def isRequeued: Boolean = this == Requeued
  def isRequeuedHeld: Boolean = this == RequeuedHeld
  def isQueued: Boolean = this == Queued
  def isQueuedHeld: Boolean = this == QueuedHeld
  def isRunning: Boolean = this == Running
  def isSuspended: Boolean = this == Suspended
  def isUndetermined: Boolean = this == Undetermined
  def isDoneUndetermined: Boolean = this == DoneUndetermined
  def isCommandResult: Boolean = this.isInstanceOf[CommandResult]

  def notFinished: Boolean = !isFinished

  def isFinished: Boolean = isFailed || isDone || isDoneUndetermined || isCommandResult
}

object DrmStatus {
  
  case object Done extends DrmStatus
  case object Queued extends DrmStatus
  case object QueuedHeld extends DrmStatus
  case object Requeued extends DrmStatus
  case object RequeuedHeld extends DrmStatus
  case object Running extends DrmStatus

  final case class CommandResult(exitStatus: Int) extends DrmStatus
  
  case object Failed extends DrmStatus
  case object DoneUndetermined extends DrmStatus
  case object Suspended extends DrmStatus
  case object Undetermined extends DrmStatus

  import Session._

  def fromDrmStatusCode(status: Int): DrmStatus = status match {
    case QUEUED_ACTIVE                                              => Queued
    case SYSTEM_ON_HOLD | USER_ON_HOLD | USER_SYSTEM_ON_HOLD        => QueuedHeld
    case RUNNING                                                    => Running
    case SYSTEM_SUSPENDED | USER_SUSPENDED | USER_SYSTEM_SUSPENDED  => Suspended
    case DONE                                                       => Done
    case FAILED                                                     => Failed
    case UNDETERMINED | _                                           => Undetermined
  }

  def toJobStatus(status: DrmStatus): JobStatus = status match {
    case Done                                          => JobStatus.WaitingForOutputs
    case CommandResult(exitStatus)                     => JobStatus.fromExitCode(exitStatus)
    case DoneUndetermined                              => JobStatus.Failed
    case Failed                                        => JobStatus.Failed
    case Queued | QueuedHeld | Requeued | RequeuedHeld => JobStatus.Submitted
    case Running                                       => JobStatus.Running
    case Suspended                                     => JobStatus.Failed
    case Undetermined                                  => JobStatus.Unknown
  }

  def toJobResult(status: DrmStatus): Option[JobResult] = status match {
    //Done => JobResult.Success?
    case CommandResult(exitStatus)      => Some(JobResult.CommandResult(exitStatus))
    case DoneUndetermined               => Some(JobResult.Failure)
    case Failed                         => Some(JobResult.Failure)
    case Suspended                      => Some(JobResult.Failure)
    case _                              => None
  }
}
