package loamstream.drm.lsf

import org.scalatest.FunSuite

import loamstream.drm.DrmStatus

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
    import LsfStatus._
        
    def doTest(exitCodeOpt: Option[Int]): Unit = {
      assert(CommandResult(42).toDrmStatus(exitCodeOpt) === DrmStatus.CommandResult(42))
      assert(CommandResult(0).toDrmStatus(exitCodeOpt) === DrmStatus.CommandResult(0))
    
      assert(Pending.toDrmStatus(exitCodeOpt) === DrmStatus.Queued)
      assert(Provisioned.toDrmStatus(exitCodeOpt) === DrmStatus.Running)
      assert(SuspendedWhilePending.toDrmStatus(exitCodeOpt) === DrmStatus.Suspended)
      assert(Running.toDrmStatus(exitCodeOpt) === DrmStatus.Running)
      assert(SuspendedWhileRunning.toDrmStatus(exitCodeOpt) === DrmStatus.Suspended)
      assert(Suspended.toDrmStatus(exitCodeOpt) === DrmStatus.Suspended)
      assert(Done.toDrmStatus(exitCodeOpt) === DrmStatus.CommandResult(0))
      exitCodeOpt match {
        case Some(exitCode) => {
          assert(Exited.toDrmStatus(exitCodeOpt) === DrmStatus.CommandResult(exitCode))
        }
        case None => assert(Exited.toDrmStatus(exitCodeOpt) === DrmStatus.Failed)
      }
      assert(Unknown.toDrmStatus(exitCodeOpt) === DrmStatus.Undetermined)
      assert(WaitingToRun.toDrmStatus(exitCodeOpt) === DrmStatus.Queued)
      assert(Zombie.toDrmStatus(exitCodeOpt) === DrmStatus.Failed)
    }
    
    doTest(Some(42))
    doTest(Some(0))
    doTest(None)
  }
}
