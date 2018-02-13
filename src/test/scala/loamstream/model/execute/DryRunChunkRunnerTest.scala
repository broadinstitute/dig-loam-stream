package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.RxMockJob
import loamstream.model.jobs.commandline.CommandLineJob
import java.nio.file.Paths

/**
 * @author clint
 * Feb 12, 2018
 */
final class DryRunChunkRunnerTest extends FunSuite {
  private def newRunner = new DryRunChunkRunner
  
  test("maxNumJobs") {
    assert(newRunner.maxNumJobs === Int.MaxValue) 
  }
  
  test("canRun") {
    val someJob = MockJob(JobStatus.Succeeded)
    val someOtherJob = RxMockJob("foo")
    val yetAnotherJob = CommandLineJob("some command line", Paths.get("."), Environment.Local)
     
    val runner = newRunner
    
    //NB: It's not possible to test that DryRunChunkRunner.canRun returns true for "everything", so we try
    //a few kinds of jobs.
    assert(runner.canRun(someJob))
    assert(runner.canRun(someOtherJob))
    assert(runner.canRun(someOtherJob))
  }
  
  test("run") {
    fail("TODO")
  }
}
