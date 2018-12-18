package loamstream.model.execute

import org.scalatest.FunSuite

import loamstream.TestHelpers.path
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.DataHandle


/**
 * @author clint
 * Nov 14, 2018
 */
final class RunsIfNoOutputsJobFilterTest extends FunSuite {
  test("shouldRun") {
    val noOutputs = MockJob(JobStatus.Succeeded)
    
    assert(noOutputs.outputs.isEmpty)
    
    val someOutputs = MockJob(JobStatus.Succeeded, outputs = Set(DataHandle.PathHandle(path("/foo/bar"))))
    
    assert(someOutputs.outputs.nonEmpty)
    
    assert(RunsIfNoOutputsJobFilter.shouldRun(noOutputs) === true)
    assert(RunsIfNoOutputsJobFilter.shouldRun(someOutputs) === false)
  }
}
