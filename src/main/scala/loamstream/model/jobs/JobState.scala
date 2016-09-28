package loamstream.model.jobs

import loamstream.model.jobs.JobState.{Failed, Skipped, Succeeded, CommandFailed}

/**
 * @author clint
 * date: Aug 2, 2016
 */
trait JobState {
  def isSuccess: Boolean = this == Succeeded || this == Skipped
  
  def isFailure: Boolean = this match {
    case Failed | CommandFailed(_) => true
    case _ => false
  }
  
  def isFinished: Boolean = isSuccess || isFailure
}

object JobState {
  case object NotStarted extends JobState
  case object Running extends JobState
  case object Failed extends JobState
  final case class CommandFailed(exitStatus: Int) extends JobState
  case object Succeeded extends JobState
  final case class CommandSucceeded(exitStatus: Int) extends JobState
  case object Skipped extends JobState
  case object Unknown extends JobState
}