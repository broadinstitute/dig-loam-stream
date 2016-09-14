package loamstream.model.jobs

/**
 * @author clint
 * date: Aug 2, 2016
 */
trait JobState {
  def isSuccess: Boolean = this == JobState.Succeeded
  def isFinished: Boolean = this == JobState.Succeeded || this == JobState.Failed
  def notFinished: Boolean = !isFinished
}

object JobState {
  case object NotStarted extends JobState
  case object Running extends JobState
  case object Failed extends JobState
  case object Succeeded extends JobState
  case object Unknown extends JobState
}