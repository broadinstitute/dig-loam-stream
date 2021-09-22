package loamstream.drm.slurm

import loamstream.drm.DrmStatus
import loamstream.model.jobs.TerminationReason
import loamstream.util.Tries
import scala.util.Try
import loamstream.util.Options

/**
 * @author clint
 * Jun 1, 2021
 */
sealed abstract class SlurmStatus(
    val shortName: String, 
    val fullName: String, 
    val drmStatus: DrmStatus,
    val terminationReason: Option[TerminationReason] = None) {
  
  def this(
      shortName: String, 
      fullName: String, 
      drmStatus: DrmStatus,
      terminationReason: TerminationReason) = this(shortName, fullName, drmStatus, Option(terminationReason))
}

object SlurmStatus {
  case object BootFail extends SlurmStatus("BF", "BOOT_FAIL", DrmStatus.Failed)
  //Job terminated due to launch failure, typically due to a hardware failure (e.g. unable to boot the node or block and the job can not be requeued).

  case object Cancelled extends SlurmStatus("CA", "CANCELLED", DrmStatus.Failed, TerminationReason.UserRequested)
  //Job was explicitly cancelled by the user or system administrator. The job may or may not have been initiated.

  case object Completed extends SlurmStatus("CD", "COMPLETED", DrmStatus.CommandResult(0))
  //Job has terminated all processes on all nodes with an exit code of zero.

  // NB: It's not at all clear what this means.  More info at 
  // https://slurm.schedmd.com/troubleshoot.html#completing
  // This seems to mean a job that Slurm is trying to kill but can't.  In practice, it only gets 
  // assigned to commands that end up finishing normally.  I'll map this to `Done`, which will
  // get LS to treat this a success.  If the job's exit code ends up being non-zero, it can still
  //be deemed a failure that way. -Clint Sep 22, 2021
  case object Completing extends SlurmStatus("CG", "COMPLETING", DrmStatus.Done)
  //Job is in the process of completing. Some processes on some nodes may still be active. 

  case object DeadLine extends SlurmStatus("DL", "DEADLINE", DrmStatus.Failed, TerminationReason.RunTime)
  //Job terminated on deadline.

  case object Failed extends SlurmStatus("F", "FAILED", DrmStatus.Failed)
  //Job terminated with non-zero exit code or other failure condition.

  case object NodeFail extends SlurmStatus("NF", "NODE_FAIL", DrmStatus.Failed, TerminationReason.Unknown)
  //Job terminated due to failure of one or more allocated nodes.

  case object OutOfMemory extends SlurmStatus("OOM", "OUT_OF_MEMORY", DrmStatus.Failed, TerminationReason.Memory)
  //Job experienced out of memory error.

  case object Pending extends SlurmStatus("PD", "PENDING", DrmStatus.Queued)
  //Job is awaiting resource allocation.

  case object Preempted extends SlurmStatus("PR", "PREEMPTED", DrmStatus.Failed, TerminationReason.Unknown)
  //Job terminated due to preemption.

  case object Running extends SlurmStatus("R", "RUNNING", DrmStatus.Running)
  //Job currently has an allocation.

  case object Requeued extends SlurmStatus("RQ", "REQUEUED", DrmStatus.Requeued)
  //Job was requeued.

  case object Resizing extends SlurmStatus("RS", "RESIZING", DrmStatus.Running) //TODO: is DrmStatus mapping appropriate?
  //Job is about to change size.

  case object Revoked extends SlurmStatus("RV", "REVOKED", DrmStatus.Undetermined) //TODO: Research what this means
  //Sibling was removed from cluster due to other cluster starting the job.

  case object Suspended extends SlurmStatus("S", "SUSPENDED", DrmStatus.Suspended)
  //Job has an allocation, but execution has been suspended and CPUs have been released for other jobs.

  case object Timeout extends SlurmStatus("TO", "TIMEOUT", DrmStatus.Failed, TerminationReason.RunTime)
  //Job terminated upon reaching its time limit.
  
  lazy val values: Iterable[SlurmStatus] = Set(
      BootFail, Cancelled, Completed, Completing, DeadLine, Failed, NodeFail, OutOfMemory, 
      Pending, Preempted, Running, Requeued, Resizing, Revoked, Suspended, Timeout)

  private lazy val byShortName: Map[String, SlurmStatus] = values.iterator.map(s => s.shortName -> s).toMap
  
  private lazy val byFullName: Map[String, SlurmStatus] = values.iterator.map(s => s.fullName -> s).toMap
  
  def fromShortName(shortName: String): Option[SlurmStatus] = byShortName.get(shortName.trim.toUpperCase)
  
  def tryFromShortName(shortName: String): Try[SlurmStatus] = {
    Options.toTry(fromShortName(shortName))(s"Couldn't find SLURM status for short name '$shortName'")
  }
  
  def fromFullName(fullName: String): Option[SlurmStatus] = byFullName.get(fullName.trim.toUpperCase)
  
  def tryFromFullName(fullName: String): Try[SlurmStatus] = {
    Options.toTry(fromFullName(fullName))(s"Couldn't find SLURM status for full name '$fullName'")
  }
}