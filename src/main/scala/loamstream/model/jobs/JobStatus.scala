package loamstream.model.jobs

import loamstream.util.Loggable

/**
 * @author kyuksel
 *         date: 3/22/17
 */
sealed trait JobStatus {
  def isSuccess: Boolean

  def isFailure: Boolean

  def isFinished: Boolean = isSuccess || isFailure

  def notFinished: Boolean = !isFinished
}

object JobStatus extends Loggable {

  sealed abstract class Success(
                      override val isSuccess: Boolean = true,
                      override val isFailure: Boolean = false) extends JobStatus

  sealed abstract class Failure(
                      override val isSuccess: Boolean = false,
                      override val isFailure: Boolean = true) extends JobStatus

  sealed abstract class NeitherSuccessNorFailure(
                                       override val isSuccess: Boolean = false,
                                       override val isFailure: Boolean = false) extends JobStatus

  case object Succeeded extends Success
  case object Skipped extends Success
  case object Failed extends Failure
  case object NotStarted extends NeitherSuccessNorFailure
  case object Submitted extends NeitherSuccessNorFailure
  case object Terminated extends NeitherSuccessNorFailure
  case object Running extends NeitherSuccessNorFailure
  case object Unknown extends NeitherSuccessNorFailure

  def withName(name: String): JobStatus = name match {
    case "Succeeded" => Succeeded
    case "Skipped" => Skipped
    case "Failed" => Failed
    case "NotStarted" => NotStarted
    case "Submitted" => Submitted
    case "Terminated" => Terminated
    case "Running" => Running
    case "Unknown" => Unknown
    case _ => warn(s"$name is not one of known JobStatus'es; mapping to ${JobStatus.Unknown}")
              Unknown
  }
}
