package loamstream.model.jobs

import loamstream.util.TypeBox
import scala.reflect.runtime.universe.Type
import org.apache.tools.ant.taskdefs.condition.IsFailure

/**
 * @author clint
 * date: Aug 2, 2016
 */
sealed trait JobState {
  import JobState._
  
  def isSuccess: Boolean
  
  def isFailure: Boolean
  
  def isFinished: Boolean = isSuccess || isFailure
}

object JobState {
  case object NotStarted extends NeitherSuccessNorFailure
  case object Running extends NeitherSuccessNorFailure
  case object Failed extends FailureState
  case object Succeeded extends SuccessState
  case object Skipped extends SuccessState
  case object Unknown extends NeitherSuccessNorFailure
  
  final case class CommandResult(exitStatus: Int) extends JobState {
    override def isSuccess: Boolean = isSuccessStatusCode(exitStatus)
    
    override def isFailure: Boolean = isFailureStatusCode(exitStatus)
  }
  
  final case class CommandInvocationFailure(e: Throwable) extends FailureState
  
  final case class FailedWithException(e: Throwable) extends FailureState
  
  //NB: Needed to support native jobs
  final case class ValueSuccess[A](value: A, typeBox: TypeBox[A]) extends SuccessState {
    def tpe: Type = typeBox.tpe
  }

  sealed abstract class SimpleJobState(
      override val isSuccess: Boolean, 
      override val isFailure: Boolean) extends JobState
      
  sealed abstract class FailureState extends SimpleJobState(isSuccess = false, isFailure = true)
  sealed abstract class SuccessState extends SimpleJobState(isSuccess = true, isFailure = false)
  sealed abstract class NeitherSuccessNorFailure extends SimpleJobState(isSuccess = false, isFailure = false)
  
  //TODO: Make this more flexible?
  private def isFailureStatusCode(i: Int): Boolean = !isSuccessStatusCode(i)
  
  //TODO: Make this more flexible?
  private def isSuccessStatusCode(i: Int): Boolean = i == 0
}