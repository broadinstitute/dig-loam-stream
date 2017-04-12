package loamstream.model.jobs

import loamstream.util.Loggable

/**
 * @author kyuksel
 *         date: 3/22/17
 */
sealed trait JobStatus {
  def isSuccess: Boolean

  def isFailure: Boolean

  def isFinished: Boolean = isSuccess || isFailure || isTerminal

  def notFinished: Boolean = !isFinished
  
  def isSkipped: Boolean = this == JobStatus.Skipped
  
  def isPermanentFailure: Boolean = this == JobStatus.FailedPermanently

  def isTerminal: Boolean = false
  
  def isRunning: Boolean = this == JobStatus.Running
  
  def notRunning: Boolean = !isRunning
}

object JobStatus extends Loggable {

  case object Succeeded extends Success with IsTerminal
  case object Skipped extends Success with IsTerminal
  case object Failed extends Failure
  case object FailedWithException extends Failure
  case object Terminated extends Failure
  case object NotStarted extends NeitherSuccessNorFailure
  case object Submitted extends NeitherSuccessNorFailure
  case object Running extends NeitherSuccessNorFailure
  case object Unknown extends NeitherSuccessNorFailure
  case object FailedPermanently extends Failure with IsTerminal

  def fromString(s: String): Option[JobStatus] = namesToInstances.get(s.toLowerCase.trim)

  def fromExitCode(code: Int): JobStatus = {
    if (JobResult.isSuccessExitCode(code)) { Succeeded }
    else { Failed }
  }
  
  sealed trait IsTerminal { self: JobStatus =>
    override val isTerminal: Boolean = true
  }
  
  sealed abstract class Success(
                      override val isSuccess: Boolean = true,
                      override val isFailure: Boolean = false) extends JobStatus 

  sealed abstract class Failure(
                      override val isSuccess: Boolean = false,
                      override val isFailure: Boolean = true) extends JobStatus

  sealed abstract class NeitherSuccessNorFailure(
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
    "permanentfailure" -> FailedPermanently)
}
