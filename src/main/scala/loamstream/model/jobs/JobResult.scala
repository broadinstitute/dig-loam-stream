package loamstream.model.jobs

import loamstream.util.TypeBox
import scala.reflect.runtime.universe.Type
import loamstream.model.execute.Resources

/**
 * @author clint
 * date: Aug 2, 2016
 */
sealed trait JobResult {
  def resources: Option[Resources] = None
}

object JobResult {
  case object NoResult extends JobResult

  final case class CommandResult(exitStatus: Int, override val resources: Option[Resources]) extends JobResult {
    def withResources(rs: Resources): CommandResult = copy(resources = Option(rs))
  }

  final case class CommandInvocationFailure(e: Throwable) extends JobResult

  final case class FailedWithException(e: Throwable) extends JobResult

  //NB: Needed to support native jobs
  final case class ValueSuccess[A](value: A, typeBox: TypeBox[A]) extends JobResult {
    def tpe: Type = typeBox.tpe
  }

  private def isFailureStatusCode(i: Int): Boolean = !isSuccessStatusCode(i)
  
  private def isSuccessStatusCode(i: Int): Boolean = i == 0

  val DummyExitCode: Int = -1
}
