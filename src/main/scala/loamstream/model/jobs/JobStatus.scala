package loamstream.model.jobs

/**
 * @author kyuksel
 *         date: 3/22/17
 */
trait JobStatus {
  def isSuccess: Boolean

  def isFailure: Boolean

  def isFinished: Boolean = isSuccess || isFailure

  def notFinished: Boolean = !isFinished
}

object JobStatus {

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
}
