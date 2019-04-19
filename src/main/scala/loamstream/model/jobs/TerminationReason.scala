package loamstream.model.jobs

/**
 * @author clint
 * Apr 16, 2019
 * 
 * An ADT for the reasons why a job was killed by the underlying system (Uger/LSF/Google) 
 */
sealed trait TerminationReason

object TerminationReason {
  //A was killed because it ran too long 
  case object RunTime extends TerminationReason
  //A was killed because it used too much CPU time
  case object CpuTime extends TerminationReason
  //A was killed because it used too much RAM
  case object Memory extends TerminationReason
  //A was killed because the user requested it (with ctrl-c, say)
  case object UserRequested extends TerminationReason
  
  val values: Set[TerminationReason] = Set(RunTime, CpuTime, Memory, UserRequested)
}
