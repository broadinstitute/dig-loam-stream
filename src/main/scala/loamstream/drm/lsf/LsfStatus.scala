package loamstream.drm.lsf

import loamstream.drm.DrmStatus
import loamstream.model.execute.Resources.LsfResources

/**
 * @author clint
 * May 17, 2018
 *
 * See https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.2/lsf_command_ref/bjobs.1.html
 *
 * PEND The job is pending, that is, it has not yet been started.
 *
 * PROV The job has been dispatched to a power-saved host that is waking up. Before the job can be sent to the sbatchd, 
 *  it is in a PROV state.
 *
 * PSUSP The job has been suspended, either by its owner or the LSF administrator, while pending.
 *
 * RUN the job is currently running.
 *
 * USUSP The job has been suspended, either by its owner or the LSF administrator, while running.
 *
 * SSUSP The job has been suspended by LSF. 
 *  The job has been suspended by LSF due to either of the following two causes:
 *   The load conditions on the execution host or hosts have exceeded a threshold according to the loadStop vector 
 *   defined for the host or queue.
 *   The run window of the job's queue is closed. See bqueues(1), bhosts(1), and lsb.queues(5).
 *
 * DONE The job has terminated with status of 0.
 *
 * EXIT The job has terminated with a non-zero status. 
 *   It may have been aborted due to an error in its execution, or killed by its owner or the LSF administrator.
 *   For example, exit code 131 means that the job exceeded a configured resource usage limit and LSF killed the job.
 *
 * UNKWN mbatchd has lost contact with the sbatchd on the host on which the job runs.
 *
 * WAIT For jobs submitted to a chunk job queue, members of a chunk job that are waiting to run.
 *
 * ZOMBI A job becomes ZOMBI if:
 *   A non-rerunnable job is killed by bkill while the sbatchd on the execution host is unreachable and the job is 
 *   shown as UNKWN.
 *   The host on which a rerunnable job is running is unavailable and the job has been requeued by LSF with a new job 
 *   ID, as if the job were submitted as a new job.
 *   After the execution host becomes available, LSF tries to kill the ZOMBI job. Upon successful termination of the 
 *   ZOMBI job, the job’s status is changed to EXIT.
 *   With MultiCluster, when a job running on a remote execution cluster becomes a ZOMBI job, the execution cluster 
 *   treats the job the same way as local ZOMBI jobs. In addition, it notifies the submission cluster that the job 
 *   is in ZOMBI state and the submission cluster requeues the job.
 */
sealed abstract class LsfStatus(val lsfName: Option[String]) {
  import LsfStatus._
  
  def this(name: String) = this(Option(name))
  
  def toDrmStatus(exitCodeOpt: Option[Int], resourcesOpt: Option[LsfResources]): DrmStatus = {
    (this, exitCodeOpt) match {
      case (Pending, _) => DrmStatus.Queued
      case (Provisioned, _) => DrmStatus.Running
      case (SuspendedWhilePending, _) => DrmStatus.Suspended(resourcesOpt)
      case (Running, _) => DrmStatus.Running
      case (SuspendedWhileRunning, _) => DrmStatus.Suspended(resourcesOpt)
      case (Suspended, _) => DrmStatus.Suspended()
      //TODO: Command result?
      case (Done, _) => DrmStatus.CommandResult(0, resourcesOpt)
      //Handle different exit codes, say 131 to indicate job was killed by LSF?
      case (Exited, Some(exitCode)) => DrmStatus.CommandResult(exitCode, resourcesOpt)
      case (Exited, None) => DrmStatus.Failed()
      case (Unknown, _) => DrmStatus.Undetermined(resourcesOpt)
      //TODO: ???
      case (WaitingToRun, _) => DrmStatus.Queued
      //TODO: ???
      case (Zombie, _) => DrmStatus.Failed(resourcesOpt)
      case (CommandResult(exitCode), _) => DrmStatus.CommandResult(exitCode, resourcesOpt)
    }
  }
}

object LsfStatus {

  case object Pending extends LsfStatus("PEND")
  case object Provisioned extends LsfStatus("PROV")
  case object SuspendedWhilePending extends LsfStatus("PSUSP")
  case object Running extends LsfStatus("RUN")
  case object SuspendedWhileRunning extends LsfStatus("USUSP")
  case object Suspended extends LsfStatus("SSUSP")
  case object Done extends LsfStatus("DONE")
  case object Exited extends LsfStatus("EXIT")
  case object Unknown extends LsfStatus("UNKWN")
  case object WaitingToRun extends LsfStatus("WAIT")
  case object Zombie extends LsfStatus("ZOMBI")
  
  final case class CommandResult(exitCode: Int) extends LsfStatus(None)

  def fromString(lsfName: String): Option[LsfStatus] = byName.get(lsfName.trim.toUpperCase)

  private lazy val byName: Map[String, LsfStatus] = {
    val named = Seq(
      Pending,
      Provisioned,
      SuspendedWhilePending,
      Running,
      SuspendedWhileRunning,
      Suspended,
      Done,
      Exited,
      Unknown,
      WaitingToRun,
      Zombie)
      
    named.zip(named.map(_.lsfName)).collect {
      case (status, Some(lsfName)) => (lsfName -> status)
    }.toMap
  }
}
