package loamstream.model.jobs

import scala.reflect.ClassTag
import loamstream.util.Classes

/**
 * @author clint
 * Apr 16, 2019
 * 
 * An ADT for the reasons why a job was killed by the underlying system (Uger/LSF/Google) 
 */
sealed abstract class TerminationReason(
    val raw: Option[String], 
    companion: TerminationReason.Companion[_]) {
  
  import TerminationReason._
  
  final def name: String = companion.Name
  
  final def isRunTime: Boolean = this.isInstanceOf[RunTime]
  final def isCpuTime: Boolean = this.isInstanceOf[CpuTime]
  final def isMemory: Boolean = this.isInstanceOf[Memory]
  final def isUserRequested: Boolean = this.isInstanceOf[UserRequested]
  final def isUnclassified: Boolean = this.isInstanceOf[Unclassified]
  final def isUnknown: Boolean = this.isInstanceOf[Unknown]
}

object TerminationReason {
  abstract class Companion[A : ClassTag] {
    val Name: String = Classes.simpleNameOf[A]
  }
  
  //A was killed because it ran too long 
  final case class RunTime(override val raw: Option[String] = None) extends TerminationReason(raw, RunTime)
  object RunTime extends Companion[RunTime]
  
  //A was killed because it used too much CPU time
  final case class CpuTime(override val raw: Option[String] = None) extends TerminationReason(raw, CpuTime)
  object CpuTime extends Companion[CpuTime]
  
  //A was killed because it used too much RAM
  final case class Memory(override val raw: Option[String] = None) extends TerminationReason(raw, Memory)
  object Memory extends Companion[Memory]
  
  //A was killed because the user requested it (with ctrl-c, say)
  final case class UserRequested(override val raw: Option[String] = None) extends TerminationReason(raw, UserRequested)
  object UserRequested extends Companion[UserRequested]
  
  final case class Unclassified(override val raw: Option[String] = None) extends TerminationReason(raw, Unclassified)
  object Unclassified extends Companion[Unclassified]
  
  final case class Unknown(override val raw: Option[String] = None) extends TerminationReason(raw, Unknown)
  object Unknown extends Companion[Unknown]
  
  def fromNameAndRawValue(name: String, raw: Option[String]): Option[TerminationReason] = name match {
    case RunTime.Name => Some(RunTime(raw))
    case CpuTime.Name => Some(CpuTime(raw))
    case Memory.Name => Some(Memory(raw))
    case UserRequested.Name => Some(UserRequested(raw))
    case Unclassified.Name => Some(Unclassified(raw))
    case Unknown.Name => Some(Unknown(raw))
    case _ => None
  }
}
