package loamstream.drm.lsf

import scala.util.Success

import org.scalatest.FunSuite

import loamstream.util.RunResults
import loamstream.util.Tries
import loamstream.drm.SessionSource

/**
 * @author clint
 * May 22, 2018
 */
final class BkillJobKillerTest extends FunSuite {
  test("makeTokens") {
    val executable = "foo"
    val user = "asdf"
    
    assert(BkillJobKiller.makeTokens(SessionSource.Noop, executable, user) === Seq(executable, "-u", user, "0"))
  }
  
  test("killAllJobs - happy path") {
    //Basically the best we can do is test that we don't throw
    
    val killer = new BkillJobKiller( () => 
      Success(RunResults("foo", 0, Nil, Nil))
    )
    
    killer.killAllJobs()
  }
  
  test("killAllJobs - non-zero exit code") {
    //Basically the best we can do is test that we don't throw
    
    val killer = new BkillJobKiller( () => 
      Success(RunResults("foo", 42, Nil, Nil))
    )
    
    killer.killAllJobs()
  }
  
  test("killAllJobs - something threw") {
    //Basically the best we can do is test that we don't throw
    
    val killer = new BkillJobKiller( () => 
      Tries.failure("blerg")
    )
    
    killer.killAllJobs()
  }
}
