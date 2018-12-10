package loamstream.model.jobs

import loamstream.util.ExitCodes
import loamstream.util.TypeBox

import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.JobResult.Success

/**
 * @author clint
 * date: Aug 2, 2016
 */
sealed trait JobResult {
  def isSuccess: Boolean = this match {
    case CommandResult(exitCode) => ExitCodes.isSuccess(exitCode)
    case Success => true
    case _ => false
  }

  def isFailure: Boolean = !isSuccess

  def toJobStatus: JobStatus = {
    if (isSuccess) { JobStatus.Succeeded }
    else { JobStatus.Failed }
  }
}

object JobResult {
  val DummyExitCode: Int = -1

  case object Success extends JobResult

  case object Failure extends JobResult

  final case class CommandResult(exitCode: Int) extends JobResult

  final case class CommandInvocationFailure(e: Throwable) extends JobResult

  final case class FailureWithException(e: Throwable) extends JobResult
}
