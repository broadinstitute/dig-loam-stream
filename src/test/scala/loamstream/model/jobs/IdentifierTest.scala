package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.execute.LocalSettings

/**
 * @author clint
 * Nov 12, 2020
 */
final class IdentifierTest extends FunSuite {
  test("from - command line job") {
    val cmd = "asldkjalsdkjlaksdj"
    
    val job = CommandLineJob(commandLineString = cmd, initialSettings = LocalSettings)
    
    assert(Identifier.from(job) === Some(cmd))
  }
  
  test("from - native job") {
    val noNameJob = NativeJob(body = () => (), initialSettings = LocalSettings, nameOpt = None)
    
    assert(Identifier.from(noNameJob) === None)
    
    val name = "asldkjaslkdjlkasjd"
    
    val namedJob = NativeJob(body = () => (), initialSettings = LocalSettings, nameOpt = Some(name))
    
    assert(Identifier.from(namedJob) === Some(name))
  }
  
  test("from - other job") {
    val job = MockJob(JobStatus.Succeeded)
    
    assert(Identifier.from(job) === None)
  }
}
