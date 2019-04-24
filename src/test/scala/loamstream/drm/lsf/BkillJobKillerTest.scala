package loamstream.drm.lsf

import org.scalatest.FunSuite
import scala.util.Success
import loamstream.util.Tries
import loamstream.util.RunResults

/**
 * @author clint
 * May 22, 2018
 */
final class BkillJobKillerTest extends FunSuite {
  test("makeTokens") {
    val executable = "foo"
    val user = "asdf"
    
    assert(BkillJobKiller.makeTokens(executable, user) === Seq(executable, "-u", user, "0"))
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
