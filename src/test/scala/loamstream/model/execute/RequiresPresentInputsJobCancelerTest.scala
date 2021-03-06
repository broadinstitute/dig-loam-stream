package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.DataHandle
import loamstream.TestHelpers
import loamstream.model.jobs.JobStatus

/**
 * @author clint
 * Nov 13, 2018
 */
final class RequiresPresentInputsJobCancelerTest extends FunSuite {
  import TestHelpers.path
  
  test("shouldCancel - job with no inputs") {
    val noInputs = MockJob(JobResult.Success)
    
    assert(noInputs.inputs.isEmpty)
    
    assert(RequiresPresentInputsJobCanceler.shouldCancel(noInputs) === false)
  }
  
  test("shouldCancel - job with all inputs present") {
    val inputs: Set[DataHandle] = Set(DataHandle.PathHandle(path("src/main")), DataHandle.PathHandle(path("src/test")))
    
    val presentInputs = MockJob(JobStatus.Succeeded, inputs = inputs)
    
    assert(presentInputs.inputs.nonEmpty)
    assert(presentInputs.inputs.forall(_.isPresent))
    
    assert(RequiresPresentInputsJobCanceler.shouldCancel(presentInputs) === false)
  }
  
  test("shouldCancel - job with all inputs missing") {
    val missing0 = DataHandle.PathHandle(path("/alskdjalksdjlkasjd"))
    val missing1 = DataHandle.PathHandle(path("/skdjfhksdjhfkjsdjf"))
    
    val inputs: Set[DataHandle] = Set(missing0, missing1)
    
    val missingInputs = MockJob(JobStatus.Succeeded, inputs = inputs)
    
    assert(missingInputs.inputs.nonEmpty)
    assert(missingInputs.inputs.forall(_.isMissing))
    
    assert(RequiresPresentInputsJobCanceler.shouldCancel(missingInputs))
  }
  
  test("shouldCancel - job with some inputs missing") {
    val present = DataHandle.PathHandle(path("src/main"))
    val missing = DataHandle.PathHandle(path("/skdjfhksdjhfkjsdjf"))
    
    val inputs: Set[DataHandle] = Set(present, missing)
    
    val someMissingInputs = MockJob(JobStatus.Succeeded, inputs = inputs)
    
    assert(present.isPresent)
    assert(missing.isMissing)
    
    assert(RequiresPresentInputsJobCanceler.shouldCancel(someMissingInputs))
  }
}
