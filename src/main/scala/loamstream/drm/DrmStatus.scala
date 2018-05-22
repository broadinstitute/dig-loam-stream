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
  def isFailed: Boolean = this.isInstanceOf[Failed]
  def isRequeued: Boolean = this == Requeued
  def isRequeuedHeld: Boolean = this == RequeuedHeld
  def isQueued: Boolean = this == Queued
  def isQueuedHeld: Boolean = this == QueuedHeld
  def isRunning: Boolean = this == Running
  def isSuspended: Boolean = this.isInstanceOf[Suspended]
  def isUndetermined: Boolean = this.isInstanceOf[Undetermined]
  def isDoneUndetermined: Boolean = this.isInstanceOf[DoneUndetermined]

  def notFinished: Boolean = {
    isRequeued || isRequeuedHeld || isQueued || isQueuedHeld || isRunning || isSuspended || isUndetermined
  }

  def isFinished: Boolean = !notFinished
  
  def resourcesOpt: Option[DrmResources] = None
  
  def withResources(rs: DrmResources): DrmStatus = this
  
  def transformResources(f: DrmResources => DrmResources): DrmStatus = resourcesOpt match {
    case None => this
    case Some(rs) => withResources(f(rs))
  }
}

object DrmStatus {
  
  case object Done extends DrmStatus
  case object Queued extends DrmStatus
  case object QueuedHeld extends DrmStatus
  case object Requeued extends DrmStatus
  case object RequeuedHeld extends DrmStatus
  case object Running extends DrmStatus

  final case class CommandResult(
      exitStatus: Int, 
      override val resourcesOpt: Option[DrmResources]) extends DrmStatus {
    
    override def withResources(rs: DrmResources): DrmStatus = copy(resourcesOpt = Option(rs))
  }
  
  final case class Failed(override val resourcesOpt: Option[DrmResources] = None) extends DrmStatus {
    override def withResources(rs: DrmResources): DrmStatus = copy(resourcesOpt = Option(rs))
  }
  
  final case class DoneUndetermined(override val resourcesOpt: Option[DrmResources] = None) extends DrmStatus {
    override def withResources(rs: DrmResources): DrmStatus = copy(resourcesOpt = Option(rs))
  }
  
  final case class Suspended(override val resourcesOpt: Option[DrmResources] = None) extends DrmStatus {
    override def withResources(rs: DrmResources): DrmStatus = copy(resourcesOpt = Option(rs))
  }
  
  final case class Undetermined(override val resourcesOpt: Option[DrmResources] = None) extends DrmStatus {
    override def withResources(rs: DrmResources): DrmStatus = copy(resourcesOpt = Option(rs))
  }

  import Session._

  def fromDrmStatusCode(status: Int): DrmStatus = status match {
    case QUEUED_ACTIVE                                              => Queued
    case SYSTEM_ON_HOLD | USER_ON_HOLD | USER_SYSTEM_ON_HOLD        => QueuedHeld
    case RUNNING                                                    => Running
    case SYSTEM_SUSPENDED | USER_SUSPENDED | USER_SYSTEM_SUSPENDED  => Suspended()
    case DONE                                                       => Done
    case FAILED                                                     => Failed()
    case UNDETERMINED | _                                           => Undetermined()
  }

  def toJobStatus(status: DrmStatus): JobStatus = status match {
    case Done                                                       => JobStatus.Succeeded
    case CommandResult(exitStatus, _)                               => JobStatus.fromExitCode(exitStatus)
    case DoneUndetermined(resources)                                => JobStatus.Failed
    case Failed(resources)                                          => JobStatus.Failed
    case Queued | QueuedHeld | Requeued | RequeuedHeld              => JobStatus.Submitted
    case Running                                                    => JobStatus.Running
    case Suspended(resources)                                       => JobStatus.Failed
    case Undetermined(resources)                                    => JobStatus.Unknown
  }

  def toJobResult(status: DrmStatus): Option[JobResult] = status match {
    //Done => JobResult.Success?
    case CommandResult(exitStatus, resources)      => Some(JobResult.CommandResult(exitStatus))
    case DoneUndetermined(resources)               => Some(JobResult.Failure)
    case Failed(resources)                         => Some(JobResult.Failure)
    case Suspended(resources)                      => Some(JobResult.Failure)
    case _                                         => None
  }
}
