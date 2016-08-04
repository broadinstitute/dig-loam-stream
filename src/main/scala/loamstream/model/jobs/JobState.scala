package loamstream.model.jobs

/**
 * @author clint
 * date: Aug 2, 2016
 */
trait JobState {
  def isFinished: Boolean = this == JobState.Finished
}

object JobState {
  case object NotStarted extends JobState
  case object Running extends JobState
  case object Failed extends JobState
  case object Finished extends JobState
  case object Unknown extends JobState
}