package loamstream.uger

import loamstream.model.jobs.{JobResult, JobStatus}
import org.ggf.drmaa.Session
import loamstream.model.execute.Resources.UgerResources

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
    isRequeued || isRequeuedHeld || isQueued || isQueuedHeld || isRunning || isSuspended
  }

  def isFinished: Boolean = !notFinished
  
  def resourcesOpt: Option[UgerResources] = None
  
  def withResources(rs: UgerResources): UgerStatus = this
  
  def transformResources(f: UgerResources => UgerResources): UgerStatus = resourcesOpt match {
    case None => this
    case Some(rs) => withResources(f(rs))
  }
}

object UgerStatus {
  
  case object Done extends UgerStatus
  case object Queued extends UgerStatus
  case object QueuedHeld extends UgerStatus
  case object Requeued extends UgerStatus
  case object RequeuedHeld extends UgerStatus
  case object Running extends UgerStatus

  final case class CommandResult(
      exitStatus: Int, 
      override val resourcesOpt: Option[UgerResources]) extends UgerStatus {
    
    override def withResources(rs: UgerResources): UgerStatus = copy(resourcesOpt = Option(rs))
  }
  
  final case class Failed(override val resourcesOpt: Option[UgerResources] = None) extends UgerStatus {
    override def withResources(rs: UgerResources): UgerStatus = copy(resourcesOpt = Option(rs))
  }
  
  final case class DoneUndetermined(override val resourcesOpt: Option[UgerResources] = None) extends UgerStatus {
    override def withResources(rs: UgerResources): UgerStatus = copy(resourcesOpt = Option(rs))
  }
  
  final case class Suspended(override val resourcesOpt: Option[UgerResources] = None) extends UgerStatus {
    override def withResources(rs: UgerResources): UgerStatus = copy(resourcesOpt = Option(rs))
  }
  
  final case class Undetermined(override val resourcesOpt: Option[UgerResources] = None) extends UgerStatus {
    override def withResources(rs: UgerResources): UgerStatus = copy(resourcesOpt = Option(rs))
  }

  import Session._

  def fromUgerStatusCode(status: Int): UgerStatus = status match {
    case QUEUED_ACTIVE                                              => Queued
    case SYSTEM_ON_HOLD | USER_ON_HOLD | USER_SYSTEM_ON_HOLD        => QueuedHeld
    case RUNNING                                                    => Running
    case SYSTEM_SUSPENDED | USER_SUSPENDED | USER_SYSTEM_SUSPENDED  => Suspended()
    case DONE                                                       => Done
    case FAILED                                                     => Failed()
    case UNDETERMINED | _                                           => Undetermined()
  }

  def toJobStatus(status: UgerStatus): JobStatus = status match {
    case Done                                                       => JobStatus.Succeeded
    case CommandResult(exitStatus, _)                               => JobResult.toJobStatus(exitStatus)
    case DoneUndetermined(resources)                                => JobStatus.Failed
    case Failed(resources)                                          => JobStatus.Failed
    case Queued | QueuedHeld | Requeued | RequeuedHeld              => JobStatus.Submitted
    case Running                                                    => JobStatus.Running
    case Suspended(resources)                                       => JobStatus.Failed
    case Undetermined(resources)                                    => JobStatus.Failed
  }

  def toJobResult(status: UgerStatus): Option[JobResult] = status match {
    case CommandResult(exitStatus, resources)      => Some(JobResult.CommandResult(exitStatus))
    case DoneUndetermined(resources)               => Some(JobResult.Failure)
    case Failed(resources)                         => Some(JobResult.Failure)
    case Suspended(resources)                      => Some(JobResult.Failure)
    case Undetermined(resources)                   => Some(JobResult.Failure)
    case _                                         => None
  }
}
