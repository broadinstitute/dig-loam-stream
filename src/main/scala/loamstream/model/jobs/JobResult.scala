package loamstream.model.jobs

import loamstream.util.TypeBox
import scala.reflect.runtime.universe.Type
import loamstream.model.execute.Resources

/**
 * @author clint
 * date: Aug 2, 2016
 */
sealed trait JobResult {
  def isSuccess: Boolean

  def isFailure: Boolean

  def isFinished: Boolean = isSuccess || isFailure
  def notFinished: Boolean = !isFinished

  def resources: Option[Resources] = None
}

object JobResult {
  case object NotStarted extends NeitherSuccessNorFailure
  case object Running extends NeitherSuccessNorFailure
  case object Succeeded extends SuccessResult
  case object Skipped extends SuccessResult
  case object Unknown extends NeitherSuccessNorFailure

  final case class CommandResult(exitStatus: Int, override val resources: Option[Resources]) extends JobResult {
    override def isSuccess: Boolean = isSuccessStatusCode(exitStatus)

    override def isFailure: Boolean = isFailureStatusCode(exitStatus)

    def withResources(rs: Resources): CommandResult = copy(resources = Option(rs))
  }

  final case class Failed(override val resources: Option[Resources] = None) extends FailureResult

  final case class CommandInvocationFailure(e: Throwable) extends FailureResult

  final case class FailedWithException(e: Throwable) extends FailureResult

  //NB: Needed to support native jobs
  final case class ValueSuccess[A](value: A, typeBox: TypeBox[A]) extends SuccessResult {
    def tpe: Type = typeBox.tpe
  }

  sealed abstract class SimpleJobResult(
      override val isSuccess: Boolean,
      override val isFailure: Boolean) extends JobResult
      
  sealed abstract class FailureResult extends SimpleJobResult(isSuccess = false, isFailure = true)
  sealed abstract class SuccessResult extends SimpleJobResult(isSuccess = true, isFailure = false)
  sealed abstract class NeitherSuccessNorFailure extends SimpleJobResult(isSuccess = false, isFailure = false)
  
  private def isFailureStatusCode(i: Int): Boolean = !isSuccessStatusCode(i)
  
  private def isSuccessStatusCode(i: Int): Boolean = i == 0

  val DummyExitCode: Int = -1
}
