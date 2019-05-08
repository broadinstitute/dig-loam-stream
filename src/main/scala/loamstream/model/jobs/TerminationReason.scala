package loamstream.model.jobs

/**
 * @author clint
 * Apr 16, 2019
 * 
 * An ADT for the reasons why a job was killed by the underlying system (Uger/LSF/Google) 
 */
sealed abstract class TerminationReason(val raw: Option[String]) {
  import TerminationReason._
  
  def isRunTime: Boolean = this.isInstanceOf[RunTime]
  def isCpuTime: Boolean = this.isInstanceOf[CpuTime]
  def isMemory: Boolean = this.isInstanceOf[Memory]
  def isUserRequested: Boolean = this.isInstanceOf[UserRequested]
  def isUnclassified: Boolean = this.isInstanceOf[Unclassified]
  def isUnknown: Boolean = this.isInstanceOf[Unknown]
}

object TerminationReason {
  //A was killed because it ran too long 
  final case class RunTime(override val raw: Option[String] = None) extends TerminationReason(raw)
  //A was killed because it used too much CPU time
  final case class CpuTime(override val raw: Option[String] = None) extends TerminationReason(raw)
  //A was killed because it used too much RAM
  final case class Memory(override val raw: Option[String] = None) extends TerminationReason(raw)
  //A was killed because the user requested it (with ctrl-c, say)
  final case class UserRequested(override val raw: Option[String] = None) extends TerminationReason(raw)
  final case class Unclassified(override val raw: Option[String] = None) extends TerminationReason(raw)
  final case class Unknown(override val raw: Option[String] = None) extends TerminationReason(raw)
}

/*
 * LSF Termination reasons:
 * 
 * 
TERM_CHKPNT: Job was killed after checkpointing (13)
TERM_CWD_NOTEXIST: current working directory is not accessible or does not exist on the execution host (25)
TERM_CPULIMIT: Job was killed after it reached LSF CPU usage limit (12)
TERM_DEADLINE: Job was killed after deadline expires (6)
TERM_EXTERNAL_SIGNAL: Job was killed by a signal external to LSF (17)
TERM_FORCE_ADMIN: Job was killed by root or LSF administrator without time for cleanup (9)
TERM_FORCE_OWNER: Job was killed by owner without time for cleanup (8)
TERM_LOAD: Job was killed after load exceeds threshold (3)
TERM_MEMLIMIT: Job was killed after it reached LSF memory usage limit (16)
TERM_ORPHAN_SYSTEM: The orphan job was automatically terminated by LSF (27)
TERM_OWNER: Job was killed by owner (14)
TERM_PREEMPT: Job was killed after preemption (1)
TERM_PROCESSLIMIT: Job was killed after it reached LSF process limit (7)
TERM_REMOVE_HUNG_JOB: Job was removed from LSF system after it reached a job runtime limit (26)
TERM_REQUEUE_ADMIN: Job was killed and requeued by root or LSF administrator (11)
TERM_REQUEUE_OWNER: Job was killed and requeued by owner (10)
TERM_RUNLIMIT: Job was killed after it reached LSF runtime limit (5)
TERM_SWAP: Job was killed after it reached LSF swap usage limit (20)
TERM_THREADLIMIT: Job was killed after it reached LSF thread limit (21)
TERM_UNKNOWN: LSF cannot determine a termination reason. 0 is logged but TERM_UNKNOWN is not displayed (0)
TERM_WINDOW: Job was killed after queue run window closed (2)
TERM_ZOMBIE: Job exited while LSF is not available (19)
 */
