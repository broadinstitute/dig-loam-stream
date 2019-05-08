package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.model.execute.Resources.LocalResources
import java.time.Instant
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.execute.Environment
import loamstream.TestHelpers


/**
 * @author clint
 * Dec 10, 2018
 */
final class RunDataTest extends FunSuite {
  import JobStatus.Succeeded
  import JobStatus.FailedPermanently
  import JobStatus.WaitingForOutputs
  import TestHelpers.path
  
  private val dummyCommandLine = "foo --bar --baz"
  
  test("withResources") {
    val j = MockJob(Succeeded)
    
    val runDataNoResources = RunData(j, Succeeded, None, None, None, None)
    
    val localResources = LocalResources(Instant.now, Instant.now)
    
    assert(runDataNoResources.resourcesOpt === None)
    
    assert(runDataNoResources.withResources(localResources).resourcesOpt === Some(localResources))
    
    val googleResources = GoogleResources.fromClusterAndLocalResources("foo", localResources)
    
    val runDataLocalResources = RunData(j, Succeeded, None, Some(localResources), None, None)
    
    assert(runDataLocalResources.resourcesOpt === Some(localResources))
    assert(runDataLocalResources.withResources(googleResources).resourcesOpt === Some(googleResources))
  }
  
  test("cmdOpt") {
    val mockJob = MockJob(Succeeded)
    
    val runDataMockJob = RunData(mockJob, Succeeded, None, None, None, None)
    
    assert(runDataMockJob.cmdOpt === None)
    
    val clj = CommandLineJob(dummyCommandLine, executionEnvironment = Environment.Local)
    
    val runDataCommandLineJob = RunData(clj, Succeeded, None, None, None, None)
    
    assert(runDataCommandLineJob.cmdOpt === Some(dummyCommandLine))
  }
  
  test("determineJobStatus") {
    import RunData.determineJobStatus
    import JobStatus.WaitingForOutputs
    
    assert(determineJobStatus(WaitingForOutputs) === JobStatus.Succeeded)
    
    for {
      status <- (JobStatus.values - JobStatus.WaitingForOutputs) 
    } {
      assert(determineJobStatus(status) === status)
    }
  }
  
  test("toExecution - mock job, succeeded") {
    val j = MockJob(Succeeded)
    
    val localResources = LocalResources(Instant.now, Instant.now)
    
    val termReason = TerminationReason.Memory()
    
    val runData = RunData(
        job = j,
        jobStatus = Succeeded, 
        jobResult = Some(JobResult.Success),
        resourcesOpt = Some(localResources),
        outputStreamsOpt = Some(OutputStreams(path("foo"), path("bar"))),
        terminationReasonOpt = Some(termReason))
        
    val expected = Execution(
        env = j.executionEnvironment,
        cmd = None,
        status = Succeeded,
        result = Some(JobResult.Success),
        resources = Option(localResources),
        outputs = Set.empty,
        outputStreams = Some(OutputStreams(path("foo"), path("bar"))))
    
    assert(runData.toExecution === expected)
  }
  
  test("toExecution - mock job, failed") {
    val j = MockJob(Succeeded)
    
    val localResources = LocalResources(Instant.now, Instant.now)
    
    val termReason = TerminationReason.Memory()
    
    val runData = RunData(
        job = j,
        jobStatus = FailedPermanently, 
        jobResult = Some(JobResult.Failure),
        resourcesOpt = Some(localResources),
        outputStreamsOpt = Some(OutputStreams(path("foo"), path("bar"))),
        terminationReasonOpt = Some(termReason))
        
    val expected = Execution(
        env = j.executionEnvironment,
        cmd = None,
        status = FailedPermanently,
        result = Some(JobResult.Failure),
        resources = Option(localResources),
        outputs = Set.empty,
        outputStreams = Some(OutputStreams(path("foo"), path("bar"))))
    
    assert(runData.toExecution === expected)
  }
  
  test("toExecution - mock job, waiting-for-outputs") {
    val j = MockJob(WaitingForOutputs)
    
    val localResources = LocalResources(Instant.now, Instant.now)
    
    val termReason = TerminationReason.Memory()
    
    val runData = RunData(
        job = j,
        jobStatus = WaitingForOutputs, 
        jobResult = None,
        resourcesOpt = Some(localResources),
        outputStreamsOpt = Some(OutputStreams(path("foo"), path("bar"))),
        terminationReasonOpt = Some(termReason))
        
    val expected = Execution(
        env = j.executionEnvironment,
        cmd = None,
        status = Succeeded,
        result = None,
        resources = Option(localResources),
        outputs = Set.empty,
        outputStreams = Some(OutputStreams(path("foo"), path("bar"))))
    
    assert(runData.toExecution === expected)
  }
  
  test("toExecution - command line job, succeeded") {
    val clj = CommandLineJob(dummyCommandLine, executionEnvironment = Environment.Local)
    
    val localResources = LocalResources(Instant.now, Instant.now)
    
    val termReason = TerminationReason.Memory()
    
    val runData = RunData(
        job = clj,
        jobStatus = Succeeded, 
        jobResult = Some(JobResult.Success),
        resourcesOpt = Some(localResources),
        outputStreamsOpt = Some(OutputStreams(path("foo"), path("bar"))),
        terminationReasonOpt = Some(termReason))
        
    val expected = Execution(
        env = clj.executionEnvironment,
        cmd = Some(dummyCommandLine),
        status = Succeeded,
        result = Some(JobResult.Success),
        resources = Option(localResources),
        outputs = Set.empty,
        outputStreams = Some(OutputStreams(path("foo"), path("bar"))))
    
    assert(runData.toExecution === expected)
  }
  
  test("toExecution - command line job, failed") {
    val clj = CommandLineJob(dummyCommandLine, executionEnvironment = Environment.Local)
    
    val localResources = LocalResources(Instant.now, Instant.now)
    
    val termReason = TerminationReason.Memory()
    
    val runData = RunData(
        job = clj,
        jobStatus = FailedPermanently, 
        jobResult = Some(JobResult.Failure),
        resourcesOpt = Some(localResources),
        outputStreamsOpt = Some(OutputStreams(path("foo"), path("bar"))),
        terminationReasonOpt = Some(termReason))
        
    val expected = Execution(
        env = clj.executionEnvironment,
        cmd = Some(dummyCommandLine),
        status = FailedPermanently,
        result = Some(JobResult.Failure),
        resources = Option(localResources),
        outputs = Set.empty,
        outputStreams = Some(OutputStreams(path("foo"), path("bar"))))
    
    assert(runData.toExecution === expected)
  }
  
  test("toExecution - command line job, waiting-for-outputs") {
    val clj = CommandLineJob(dummyCommandLine, executionEnvironment = Environment.Local)
    
    val localResources = LocalResources(Instant.now, Instant.now)
    
    val termReason = TerminationReason.Memory()
    
    val runData = RunData(
        job = clj,
        jobStatus = WaitingForOutputs, 
        jobResult = None,
        resourcesOpt = Some(localResources),
        outputStreamsOpt = Some(OutputStreams(path("foo"), path("bar"))),
        terminationReasonOpt = Some(termReason))
        
    val expected = Execution(
        env = clj.executionEnvironment,
        cmd = Some(dummyCommandLine),
        status = Succeeded,
        result = None,
        resources = Option(localResources),
        outputs = Set.empty,
        outputStreams = Some(OutputStreams(path("foo"), path("bar"))))
    
    assert(runData.toExecution === expected)
  }
}
