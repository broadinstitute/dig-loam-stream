package loamstream.model.jobs

import loamstream.util.{ExitCodes, TypeBox}

import scala.reflect.runtime.universe.Type
import loamstream.model.jobs.JobResult.{CommandResult, Success, ValueSuccess}

/**
 * @author clint
 * date: Aug 2, 2016
 */
sealed trait JobResult {
  def isSuccess: Boolean = this match {
    case CommandResult(exitCode) => ExitCodes.isSuccess(exitCode)
    case Success | ValueSuccess(_, _) => true
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

  //NB: Needed to support native jobs
  final case class ValueSuccess[A](value: A, typeBox: TypeBox[A]) extends JobResult {
    def tpe: Type = typeBox.tpe
  }
}
