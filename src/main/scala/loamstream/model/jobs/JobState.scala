package loamstream.model.jobs

import loamstream.model.jobs.JobState.{Failed, Skipped, Succeeded}

/**
 * @author clint
 * date: Aug 2, 2016
 */
trait JobState {
  def isSuccess: Boolean = this == Succeeded || this == Skipped
  def isFailure: Boolean = this == Failed
  def isFinished: Boolean = isSuccess || isFailure
  def notFinished: Boolean = !isFinished
}

object JobState {
  case object NotStarted extends JobState
  case object Running extends JobState
  case object Failed extends JobState
  case object Succeeded extends JobState
  case object Skipped extends JobState
  case object Unknown extends JobState
}