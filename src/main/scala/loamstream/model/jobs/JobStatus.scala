package loamstream.model.jobs

import loamstream.util.Loggable
import loamstream.util.ExitCodes

/**
 * @author kyuksel
 *         date: 3/22/17
 */
sealed trait JobStatus {
  def isSuccess: Boolean

  def isFailure: Boolean
  
  def isTerminal: Boolean

  final def isFinished: Boolean = isSuccess || isFailure || isTerminal

  final def notFinished: Boolean = !isFinished
  
  final def isSkipped: Boolean = this == JobStatus.Skipped
  
  final def isPermanentFailure: Boolean = this == JobStatus.FailedPermanently

  final def isCouldNotStart: Boolean = this == JobStatus.CouldNotStart
  
  final def isRunning: Boolean = this == JobStatus.Running
  
  final def notRunning: Boolean = !isRunning
  
  final def canStopExecution: Boolean = isFailure || isCouldNotStart
}

object JobStatus extends Loggable {

  case object Succeeded extends Success
  case object Skipped extends Success
  case object Failed extends Failure
  case object FailedWithException extends Failure
  case object Terminated extends Failure
  case object NotStarted extends NeitherSuccessNorFailure
  case object Submitted extends NeitherSuccessNorFailure
  case object Running extends NeitherSuccessNorFailure
  case object WaitingForOutputs extends Success(isTerminal = false)
  case object Unknown extends NeitherSuccessNorFailure
  case object CouldNotStart extends NeitherSuccessNorFailure(isTerminal = true)
  case object FailedPermanently extends Failure(isTerminal = true)

  def values: Set[JobStatus] = namesToInstances.values.toSet
  
  def fromString(s: String): Option[JobStatus] = namesToInstances.get(s.toLowerCase.trim)

  def fromExitCode(code: Int): JobStatus = {
    if (ExitCodes.isSuccess(code)) { WaitingForOutputs }
    else { Failed }
  }
  
  protected sealed abstract class Success(
      override val isTerminal: Boolean = true,
      override val isSuccess: Boolean = true,
      override val isFailure: Boolean = false) extends JobStatus

  protected sealed abstract class Failure(
      override val isTerminal: Boolean = false,
      override val isSuccess: Boolean = false,
      override val isFailure: Boolean = true) extends JobStatus

  protected sealed abstract class NeitherSuccessNorFailure(
      override val isTerminal: Boolean = false,
      override val isSuccess: Boolean = false,
      override val isFailure: Boolean = false) extends JobStatus
  
  private lazy val namesToInstances: Map[String, JobStatus] = Map(
    "succeeded" -> Succeeded,
    "skipped" -> Skipped,
    "failed" -> Failed,
    "failedwithexception" -> FailedWithException,
    "notstarted" -> NotStarted,
    "submitted" -> Submitted,
    "terminated" -> Terminated,
    "running" -> Running,
    "unknown" -> Unknown,
    "permanentfailure" -> FailedPermanently,
    "couldnotstart" -> CouldNotStart,
    "waitingforoutputs" -> WaitingForOutputs)
}
