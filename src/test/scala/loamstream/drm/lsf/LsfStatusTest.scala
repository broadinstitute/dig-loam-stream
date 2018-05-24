package loamstream.drm.lsf

import java.time.Instant

import org.scalatest.FunSuite

import loamstream.drm.DrmStatus
import loamstream.drm.Queue
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory

/**
 * @author clint
 * May 24, 2018
 */
final class LsfStatusTest extends FunSuite {
  test("fromString") {
    import LsfStatus._
    
    def to1337Case(s: String): String = {
      "" ++ s.toLowerCase.zip(s.toUpperCase).zipWithIndex.map { case ((lower, upper), i) =>
      
        if(i % 2 == 0) lower else upper
      }
    }
    
    def doTest(s: String, expected: LsfStatus): Unit = {
      assert(fromString(s) === Some(expected))
      assert(fromString(s.toUpperCase) === Some(expected))
      assert(fromString(s.toLowerCase) === Some(expected))
      assert(fromString(s.capitalize) === Some(expected))
      assert(fromString(to1337Case(s)) === Some(expected))
    }
    
    assert(fromString("") === None)
    assert(fromString("asdf") === None)
    assert(fromString("PENDING") === None)
    
    doTest("PEND", Pending)
    doTest("PROV",Provisioned)
    doTest("PSUSP", SuspendedWhilePending)
    doTest("RUN", Running)
    doTest("USUSP", SuspendedWhileRunning)
    doTest("SSUSP", Suspended)
    doTest("DONE", Done)
    doTest("EXIT", Exited)
    doTest("UNKWN", Unknown)
    doTest("WAIT", WaitingToRun)
    doTest("ZOMBI", Zombie)
  }
  
  test("lsfName") {
    import LsfStatus._
    
    assert(CommandResult(42).lsfName === None)
    assert(CommandResult(0).lsfName === None)
    
    assert(Pending.lsfName === Some("PEND"))
    assert(Provisioned.lsfName === Some("PROV"))
    assert(SuspendedWhilePending.lsfName === Some("PSUSP"))
    assert(Running.lsfName === Some("RUN"))
    assert(SuspendedWhileRunning.lsfName === Some("USUSP"))
    assert(Suspended.lsfName === Some("SSUSP"))
    assert(Done.lsfName === Some("DONE"))
    assert(Exited.lsfName === Some("EXIT"))
    assert(Unknown.lsfName === Some("UNKWN"))
    assert(WaitingToRun.lsfName === Some("WAIT"))
    assert(Zombie.lsfName === Some("ZOMBI"))
  }
  
  test("toDrmStatus") {
    val resourcesOpt: Option[LsfResources] = Some(LsfResources(
        memory = Memory.inMb(42),
        cpuTime = CpuTime.inSeconds(99),
        node = Some("foo"),
        queue = Some(Queue("fooQueue")),
        startTime = Instant.now,
        endTime = Instant.now))
        
    import LsfStatus._
        
    def doTest(exitCodeOpt: Option[Int]): Unit = {
      assert(CommandResult(42).toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.CommandResult(42, resourcesOpt))
      assert(CommandResult(0).toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.CommandResult(0, resourcesOpt))
    
      assert(Pending.toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.Queued)
      assert(Provisioned.toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.Queued)
      assert(SuspendedWhilePending.toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.Suspended())
      assert(Running.toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.Running)
      assert(SuspendedWhileRunning.toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.Suspended(resourcesOpt))
      assert(Suspended.toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.Suspended())
      assert(Done.toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.CommandResult(0, resourcesOpt))
      exitCodeOpt match {
        case Some(exitCode) => {
          assert(Exited.toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.CommandResult(exitCode, resourcesOpt))
        }
        case None => assert(Exited.toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.Failed(resourcesOpt))
      }
      assert(Unknown.toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.Undetermined(resourcesOpt))
      assert(WaitingToRun.toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.Queued)
      assert(Zombie.toDrmStatus(exitCodeOpt, resourcesOpt) === DrmStatus.Failed(resourcesOpt))
    }
    
    doTest(Some(42))
    doTest(Some(0))
    doTest(None)
  }
}
