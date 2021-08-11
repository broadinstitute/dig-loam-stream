package loamstream.drm.slurm

import org.scalatest.FunSuite
import loamstream.drm.DrmStatus
import loamstream.model.jobs.TerminationReason
import loamstream.TestHelpers
import scala.util.Success

/**
  * @author clint
  * @date Aug 10, 2021
  *
  */
final class SlurmStatusTest extends FunSuite {
  import SlurmStatus._

  test("names/status/term-reason") {
    assert(BootFail.shortName === "BF")
    assert(BootFail.fullName === "BOOT_FAIL")
    assert(BootFail.drmStatus === DrmStatus.Failed)
    assert(BootFail.terminationReason === None)
    //Job terminated due to launch failure, typically due to a hardware failure (e.g. unable to boot the node or block and the job can not be requeued).

    assert(Cancelled.shortName === "CA")
    assert(Cancelled.fullName === "CANCELLED")
    assert(Cancelled.drmStatus === DrmStatus.Failed)
    assert(Cancelled.terminationReason === Some(TerminationReason.UserRequested))
    //Job was explicitly cancelled by the user or system administrator. The job may or may not have been initiated.

    assert(Completed.shortName === "CD")
    assert(Completed.fullName === "COMPLETED")
    assert(Completed.drmStatus === DrmStatus.CommandResult(0))
    assert(Completed.terminationReason === None)
    //Job has terminated all processes on all nodes with an exit code of zero.

    assert(Completing.shortName === "CG")
    assert(Completing.fullName === "COMPLETING")
    assert(Completing.drmStatus === DrmStatus.Failed)
    assert(Completing.terminationReason === None)
    //Job is in the process of completing. Some processes on some nodes may still be active. 

    assert(DeadLine.shortName === "DL")
    assert(DeadLine.fullName === "DEADLINE")
    assert(DeadLine.drmStatus === DrmStatus.Failed)
    assert(DeadLine.terminationReason === Some(TerminationReason.RunTime))
    //Job terminated on deadline.

    assert(Failed.shortName === "F")
    assert(Failed.fullName === "FAILED")
    assert(Failed.drmStatus === DrmStatus.Failed)
    assert(Failed.terminationReason === None)
    //Job terminated with non-zero exit code or other failure condition.

    assert(NodeFail.shortName === "NF")
    assert(NodeFail.fullName === "NODE_FAIL")
    assert(NodeFail.drmStatus === DrmStatus.Failed)
    assert(NodeFail.terminationReason === Some(TerminationReason.Unknown))
    //Job terminated due to failure of one or more allocated nodes.

    assert(OutOfMemory.shortName === "OOM")
    assert(OutOfMemory.fullName === "OUT_OF_MEMORY")
    assert(OutOfMemory.drmStatus === DrmStatus.Failed)
    assert(OutOfMemory.terminationReason === Some(TerminationReason.Memory))
    //Job experienced out of memory error.

    assert(Pending.shortName === "PD")
    assert(Pending.fullName === "PENDING")
    assert(Pending.drmStatus === DrmStatus.Queued)
    assert(Pending.terminationReason === None)
    //Job is awaiting resource allocation.

    assert(Preempted.shortName === "PR")
    assert(Preempted.fullName === "PREEMPTED")
    assert(Preempted.drmStatus === DrmStatus.Failed)
    assert(Preempted.terminationReason === Some(TerminationReason.Unknown))
    //Job terminated due to preemption.

    assert(Running.shortName === "R")
    assert(Running.fullName === "RUNNING")
    assert(Running.drmStatus === DrmStatus.Running)
    assert(Running.terminationReason === None)
    //Job currently has an allocation.

    assert(Requeued.shortName === "RQ")
    assert(Requeued.fullName === "REQUEUED")
    assert(Requeued.drmStatus === DrmStatus.Requeued)
    assert(Requeued.terminationReason === None)
    //Job was requeued.

    assert(Resizing.shortName === "RS")
    assert(Resizing.fullName === "RESIZING")
    assert(Resizing.drmStatus === DrmStatus.Running)
    assert(Resizing.terminationReason === None) //TODO: is DrmStatus mapping appropriate?
    //Job is about to change size.

    assert(Revoked.shortName === "RV")
    assert(Revoked.fullName === "REVOKED")
    assert(Revoked.drmStatus === DrmStatus.Undetermined)
    assert(Revoked.terminationReason === None) //TODO: Research what this means
    //Sibling was removed from cluster due to other cluster starting the job.

    assert(Suspended.shortName === "S")
    assert(Suspended.fullName === "SUSPENDED")
    assert(Suspended.drmStatus === DrmStatus.Suspended)
    assert(Suspended.terminationReason === None)
    //Job has an allocation, but execution has been suspended and CPUs have been released for other jobs.

    assert(Timeout.shortName === "TO")
    assert(Timeout.fullName === "TIMEOUT")
    assert(Timeout.drmStatus === DrmStatus.Failed)
    assert(Timeout.terminationReason === Some(TerminationReason.RunTime))
  }

  test("values") {
    assert(SlurmStatus.values === Set(
      BootFail, Cancelled, Completed, Completing, DeadLine, Failed, NodeFail, OutOfMemory, 
      Pending, Preempted, Running, Requeued, Resizing, Revoked, Suspended, Timeout))
  }

  import scala.language.higherKinds

  def doFromTestShouldSucceed[A, F[X]](
      input: String, 
      from: String => F[SlurmStatus])(
      expected: F[SlurmStatus]): Unit = {

    assert(from(input) === expected)
    assert(from(input.toLowerCase) === expected)
    assert(from(input.toUpperCase) === expected)
    assert(from(input.toLowerCase.capitalize) === expected)
    assert(from(TestHelpers.to1337Speak(input)) === expected)
  }

  def doFromTestShouldFail[A, F[X]](
      input: String, 
      from: String => F[SlurmStatus])(
      isFailure: F[SlurmStatus] => Boolean): Unit = {
        
    assert(isFailure(from(input)))
    assert(isFailure(from(input.toLowerCase)))
    assert(isFailure(from(input.toUpperCase)))
    assert(isFailure(from(input.toLowerCase.capitalize)))
    assert(isFailure(from(TestHelpers.to1337Speak(input))))
  }

  test("fromShortName") {
    doFromTestShouldFail("", fromShortName)(_.isEmpty)
    doFromTestShouldFail("asdfg", fromShortName)(_.isEmpty)
    doFromTestShouldFail("   ", fromShortName)(_.isEmpty)

    doFromTestShouldSucceed("BF", fromShortName)(Some(BootFail))
    doFromTestShouldSucceed("CA", fromShortName)(Some(Cancelled))
    doFromTestShouldSucceed("CD", fromShortName)(Some(Completed))
    doFromTestShouldSucceed("CG", fromShortName)(Some(Completing))
    doFromTestShouldSucceed("DL", fromShortName)(Some(DeadLine))
    doFromTestShouldSucceed("F", fromShortName)(Some(Failed))
    doFromTestShouldSucceed("NF", fromShortName)(Some(NodeFail))
    doFromTestShouldSucceed("OOM", fromShortName)(Some(OutOfMemory))
    doFromTestShouldSucceed("PD", fromShortName)(Some(Pending))
    doFromTestShouldSucceed("PR", fromShortName)(Some(Preempted))
    doFromTestShouldSucceed("R", fromShortName)(Some(Running))
    doFromTestShouldSucceed("RQ", fromShortName)(Some(Requeued))
    doFromTestShouldSucceed("RS", fromShortName)(Some(Resizing))
    doFromTestShouldSucceed("RV", fromShortName)(Some(Revoked))
    doFromTestShouldSucceed("S", fromShortName)(Some(Suspended))
    doFromTestShouldSucceed("TO", fromShortName)(Some(Timeout))
  }

  test("tryFromShortName") {
    doFromTestShouldFail("", tryFromShortName)(_.isFailure)
    doFromTestShouldFail("asdfg", tryFromShortName)(_.isFailure)
    doFromTestShouldFail("   ", tryFromShortName)(_.isFailure)

    doFromTestShouldSucceed("BF", tryFromShortName)(Success(BootFail))
    doFromTestShouldSucceed("CA", tryFromShortName)(Success(Cancelled))
    doFromTestShouldSucceed("CD", tryFromShortName)(Success(Completed))
    doFromTestShouldSucceed("CG", tryFromShortName)(Success(Completing))
    doFromTestShouldSucceed("DL", tryFromShortName)(Success(DeadLine))
    doFromTestShouldSucceed("F", tryFromShortName)(Success(Failed))
    doFromTestShouldSucceed("NF", tryFromShortName)(Success(NodeFail))
    doFromTestShouldSucceed("OOM", tryFromShortName)(Success(OutOfMemory))
    doFromTestShouldSucceed("PD", tryFromShortName)(Success(Pending))
    doFromTestShouldSucceed("PR", tryFromShortName)(Success(Preempted))
    doFromTestShouldSucceed("R", tryFromShortName)(Success(Running))
    doFromTestShouldSucceed("RQ", tryFromShortName)(Success(Requeued))
    doFromTestShouldSucceed("RS", tryFromShortName)(Success(Resizing))
    doFromTestShouldSucceed("RV", tryFromShortName)(Success(Revoked))
    doFromTestShouldSucceed("S", tryFromShortName)(Success(Suspended))
    doFromTestShouldSucceed("TO", tryFromShortName)(Success(Timeout))
  }

  test("fromFullName") {
    doFromTestShouldFail("", fromFullName)(_.isEmpty)
    doFromTestShouldFail("asdfg", fromFullName)(_.isEmpty)
    doFromTestShouldFail("   ", fromFullName)(_.isEmpty)

    doFromTestShouldSucceed("BOOT_FAIL", fromFullName)(Some(BootFail))
    doFromTestShouldSucceed("CANCELLED", fromFullName)(Some(Cancelled))
    doFromTestShouldSucceed("COMPLETED", fromFullName)(Some(Completed))
    doFromTestShouldSucceed("COMPLETING", fromFullName)(Some(Completing))
    doFromTestShouldSucceed("DEADLINE", fromFullName)(Some(DeadLine))
    doFromTestShouldSucceed("FAILED", fromFullName)(Some(Failed))
    doFromTestShouldSucceed("NODE_FAIL", fromFullName)(Some(NodeFail))
    doFromTestShouldSucceed("OUT_OF_MEMORY", fromFullName)(Some(OutOfMemory))
    doFromTestShouldSucceed("PENDING", fromFullName)(Some(Pending))
    doFromTestShouldSucceed("PREEMPTED", fromFullName)(Some(Preempted))
    doFromTestShouldSucceed("RUNNING", fromFullName)(Some(Running))
    doFromTestShouldSucceed("REQUEUED", fromFullName)(Some(Requeued))
    doFromTestShouldSucceed("RESIZING", fromFullName)(Some(Resizing))
    doFromTestShouldSucceed("REVOKED", fromFullName)(Some(Revoked))
    doFromTestShouldSucceed("SUSPENDED", fromFullName)(Some(Suspended))
    doFromTestShouldSucceed("TIMEOUT", fromFullName)(Some(Timeout))
  }

  test("tryFromFullName") {
    doFromTestShouldFail("", tryFromFullName)(_.isFailure)
    doFromTestShouldFail("asdfg", tryFromFullName)(_.isFailure)
    doFromTestShouldFail("   ", tryFromFullName)(_.isFailure)

    doFromTestShouldSucceed("BOOT_FAIL", tryFromFullName)(Success(BootFail))
    doFromTestShouldSucceed("CANCELLED", tryFromFullName)(Success(Cancelled))
    doFromTestShouldSucceed("COMPLETED", tryFromFullName)(Success(Completed))
    doFromTestShouldSucceed("COMPLETING", tryFromFullName)(Success(Completing))
    doFromTestShouldSucceed("DEADLINE", tryFromFullName)(Success(DeadLine))
    doFromTestShouldSucceed("FAILED", tryFromFullName)(Success(Failed))
    doFromTestShouldSucceed("NODE_FAIL", tryFromFullName)(Success(NodeFail))
    doFromTestShouldSucceed("OUT_OF_MEMORY", tryFromFullName)(Success(OutOfMemory))
    doFromTestShouldSucceed("PENDING", tryFromFullName)(Success(Pending))
    doFromTestShouldSucceed("PREEMPTED", tryFromFullName)(Success(Preempted))
    doFromTestShouldSucceed("RUNNING", tryFromFullName)(Success(Running))
    doFromTestShouldSucceed("REQUEUED", tryFromFullName)(Success(Requeued))
    doFromTestShouldSucceed("RESIZING", tryFromFullName)(Success(Resizing))
    doFromTestShouldSucceed("REVOKED", tryFromFullName)(Success(Revoked))
    doFromTestShouldSucceed("SUSPENDED", tryFromFullName)(Success(Suspended))
    doFromTestShouldSucceed("TIMEOUT", tryFromFullName)(Success(Timeout))
  }
}
