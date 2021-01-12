package loamstream.drm.lsf

import scala.util.Success

import org.scalatest.FunSuite

import loamstream.util.RunResults
import loamstream.util.Tries
import loamstream.drm.SessionSource
import loamstream.drm.SessionTracker
import loamstream.drm.DrmTaskId

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
  
  test("killAllJobs - happy path") {
    //Basically the best we can do is test that we don't throw
    
    val killer = BkillJobKiller { () => 
      Success(RunResults("foo", 0, Nil, Nil))
    }
    
    killer.killAllJobs()
  }
  
  test("killAllJobs - non-zero exit code") {
    //Basically the best we can do is test that we don't throw
    
    val killer = BkillJobKiller { () => 
      Success(RunResults("foo", 42, Nil, Nil))
    }
    
    killer.killAllJobs()
  }
  
  test("killAllJobs - something threw") {
    //Basically the best we can do is test that we don't throw
    
    val killer = BkillJobKiller { () =>
      Tries.failure("blerg")
    }
    
    killer.killAllJobs()
  }
}
