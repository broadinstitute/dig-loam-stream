package loamstream.drm.lsf

import scala.util.Success

import org.scalatest.FunSuite

import loamstream.drm.DrmTaskId
import loamstream.drm.SessionTracker
import loamstream.util.RunResults
import loamstream.util.Tries
import scala.util.Try
import loamstream.util.CommandInvoker

/**
 * @author clint
 * May 22, 2018
 */
final class BkillJobKillerTest extends FunSuite {
  test("makeTokens") {
    val executable = "foo"
    val user = "asdf"
    
    val sessionTracker = new SessionTracker.Default
    
    assert(sessionTracker.isEmpty)
    assert(BkillJobKiller.makeTokens(sessionTracker, executable, user) === Seq(executable, "-u", user, "0"))
    
    sessionTracker.register(Seq(DrmTaskId("X", 42), DrmTaskId("y", 1), DrmTaskId("ZZZz", 3)))
    
    assert(sessionTracker.nonEmpty)
    assert(BkillJobKiller.makeTokens(sessionTracker, executable, user) === Seq(executable, "-u", user, "0"))
  }
  
  private def makeBkillJobKiller(
      sessionTracker: SessionTracker = SessionTracker.Noop)(fn: () => Try[RunResults]): BkillJobKiller = {
    new BkillJobKiller(
      new CommandInvoker.Sync.JustOnce[Unit](
        "bkill", 
        _ => fn(),
        isSuccess = RunResults.SuccessPredicate.zeroIsSuccess), 
      sessionTracker)
  }
  
  test("killAllJobs - happy path") {
    //Basically the best we can do is test that we don't throw
    
    val killer = makeBkillJobKiller() { () => 
      Success(RunResults("foo", 0, Nil, Nil))
    }
    
    killer.killAllJobs()
  }
  
  test("killAllJobs - non-zero exit code") {
    //Basically the best we can do is test that we don't throw
    
    val killer = makeBkillJobKiller() { () => 
      Success(RunResults("foo", 42, Nil, Nil))
    }
    
    killer.killAllJobs()
  }
  
  test("killAllJobs - something threw") {
    //Basically the best we can do is test that we don't throw
    
    val killer = makeBkillJobKiller() { () =>
      Tries.failure("blerg")
    }
    
    killer.killAllJobs()
  }
}
