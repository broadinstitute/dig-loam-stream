package loamstream.model.jobs

import scala.reflect.ClassTag
import loamstream.util.Classes

/**
 * @author clint
 * Apr 16, 2019
 * 
 * An ADT for the reasons why a job was killed by the underlying system (Uger/LSF/Google) 
 */
sealed abstract class TerminationReason {
  
  import TerminationReason._
  
  //NB: This assumes implementations will be case objects
  final def name: String = toString
  
  //NB: Field's identifier needs to start with a capital letter for pattern-matching :\ 
  private[TerminationReason] val Name: String = name.toLowerCase
  
  final def isRunTime: Boolean = this == RunTime
  final def isCpuTime: Boolean = this == CpuTime
  final def isMemory: Boolean = this == Memory
  final def isUserRequested: Boolean = this == UserRequested
  final def isUnknown: Boolean = this == Unknown
}

object TerminationReason {
  //A job was killed because it ran too long 
  final case object RunTime extends TerminationReason
  
  //A job was killed because it used too much CPU time
  final case object CpuTime extends TerminationReason
  
  //A job was killed because it used too much RAM
  final case object Memory extends TerminationReason
  
  //A job was killed because the user requested it (with ctrl-c, say)
  final case object UserRequested extends TerminationReason

  //A job was killed by the underlying environment, but we don't know why.
  final case object Unknown extends TerminationReason
  
  def fromName(name: String): Option[TerminationReason] = name.trim.toLowerCase match {
    case RunTime.Name => Some(RunTime)
    case CpuTime.Name => Some(CpuTime)
    case Memory.Name => Some(Memory)
    case UserRequested.Name => Some(UserRequested)
    case Unknown.Name => Some(Unknown)
    case _ => None
  }
}
