package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.model.execute.Resources.LocalResources
import java.time.Instant
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.TestHelpers
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.GoogleSettings
import loamstream.googlecloud.ClusterConfig
import java.time.LocalDateTime


/**
 * @author clint
 * Dec 10, 2018
 */
final class RunDataTest extends FunSuite {
  import JobStatus.Succeeded
  import JobStatus.FailedPermanently
  import JobStatus.WaitingForOutputs
  import TestHelpers.path
  import TestHelpers.dummyJobDir
  
  private val dummyCommandLine = "foo --bar --baz"
  
  test("withResources") {
    val j = MockJob(Succeeded)
    
    val runDataNoResources = RunData(j, LocalSettings, Succeeded, None, None, None, None)
    
    val localResources = makeDummyResources
    
    assert(runDataNoResources.resourcesOpt === None)
    
    assert(runDataNoResources.withResources(localResources).resourcesOpt === Some(localResources))
    
    val googleResources = GoogleResources.fromClusterAndLocalResources("foo", localResources)
    
    val googleSettings = GoogleSettings("foo", ClusterConfig.default)
    
    val runDataLocalResources = RunData(j, googleSettings, Succeeded, None, Some(localResources), None, None)
    
    assert(runDataLocalResources.resourcesOpt === Some(localResources))
    assert(runDataLocalResources.withResources(googleResources).resourcesOpt === Some(googleResources))
  }
  
  test("cmdOpt") {
    val mockJob = MockJob(Succeeded)
    
    val runDataMockJob = RunData(mockJob, LocalSettings, Succeeded, None, None, None, None)
    
    assert(runDataMockJob.cmdOpt === None)
    
    val clj = CommandLineJob(dummyCommandLine, initialSettings = LocalSettings)
    
    val runDataCommandLineJob = RunData(clj, LocalSettings, Succeeded, None, None, None, None)
    
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
    
    val localResources = makeDummyResources
    
    val termReason = TerminationReason.Memory
    
    val jobDir = dummyJobDir
    
    val runData = RunData(
        job = j,
        settings = LocalSettings,
        jobStatus = Succeeded, 
        jobResult = Some(JobResult.Success),
        resourcesOpt = Some(localResources),
        jobDirOpt = Some(jobDir),
        terminationReasonOpt = Some(termReason))
        
    val expected = Execution(
        settings = j.initialSettings,
        cmd = None,
        status = Succeeded,
        result = Some(JobResult.Success),
        resources = Option(localResources),
        outputs = Set.empty,
        jobDir = Some(jobDir),
        terminationReason = Some(termReason))
    
    assert(runData.toExecution === expected)
  }
  
  test("toExecution - mock job, failed") {
    val j = MockJob(Succeeded)
    
    val localResources = makeDummyResources
    
    val termReason = TerminationReason.Memory
    
    val jobDir = dummyJobDir
    
    val runData = RunData(
        job = j,
        settings = LocalSettings,
        jobStatus = FailedPermanently, 
        jobResult = Some(JobResult.Failure),
        resourcesOpt = Some(localResources),
        jobDirOpt = Some(jobDir),
        terminationReasonOpt = Some(termReason))
        
    val expected = Execution(
        settings = j.initialSettings,
        cmd = None,
        status = FailedPermanently,
        result = Some(JobResult.Failure),
        resources = Option(localResources),
        outputs = Set.empty,
        jobDir = Some(jobDir),
        terminationReason = Some(termReason))
    
    assert(runData.toExecution === expected)
  }
  
  test("toExecution - mock job, waiting-for-outputs") {
    val j = MockJob(WaitingForOutputs)
    
    val localResources = makeDummyResources
    
    val termReason = TerminationReason.Memory
    
    val jobDir = dummyJobDir
    
    val runData = RunData(
        job = j,
        settings = LocalSettings,
        jobStatus = WaitingForOutputs, 
        jobResult = None,
        resourcesOpt = Some(localResources),
        jobDirOpt = Some(jobDir),
        terminationReasonOpt = Some(termReason))
        
    val expected = Execution(
        settings = j.initialSettings,
        cmd = None,
        status = Succeeded,
        result = None,
        resources = Option(localResources),
        outputs = Set.empty,
        jobDir = Some(jobDir),
        terminationReason = Some(termReason))
    
    assert(runData.toExecution === expected)
  }
  
  test("toExecution - command line job, succeeded") {
    val clj = CommandLineJob(dummyCommandLine, initialSettings = LocalSettings)
    
    val localResources = makeDummyResources
    
    val termReason = TerminationReason.Memory
    
    val jobDir = dummyJobDir
    
    val runData = RunData(
        job = clj,
        settings = LocalSettings,
        jobStatus = Succeeded, 
        jobResult = Some(JobResult.Success),
        resourcesOpt = Some(localResources),
        jobDirOpt = Some(jobDir),
        terminationReasonOpt = Some(termReason))
        
    val expected = Execution(
        settings = clj.initialSettings,
        cmd = Some(dummyCommandLine),
        status = Succeeded,
        result = Some(JobResult.Success),
        resources = Option(localResources),
        outputs = Set.empty,
        jobDir = Some(jobDir),
        terminationReason = Some(termReason))
    
    assert(runData.toExecution === expected)
  }
  
  test("toExecution - command line job, failed") {
    val clj = CommandLineJob(dummyCommandLine, initialSettings = LocalSettings)
    
    val localResources = makeDummyResources
    
    val termReason = TerminationReason.Memory
    
    val jobDir = dummyJobDir
    
    val runData = RunData(
        job = clj,
        settings = LocalSettings,
        jobStatus = FailedPermanently, 
        jobResult = Some(JobResult.Failure),
        resourcesOpt = Some(localResources),
        jobDirOpt = Some(jobDir),
        terminationReasonOpt = Some(termReason))
        
    val expected = Execution(
        settings = clj.initialSettings,
        cmd = Some(dummyCommandLine),
        status = FailedPermanently,
        result = Some(JobResult.Failure),
        resources = Option(localResources),
        outputs = Set.empty,
        jobDir = Some(jobDir),
        terminationReason = Some(termReason))
    
    assert(runData.toExecution === expected)
  }
  
  test("toExecution - command line job, waiting-for-outputs") {
    val clj = CommandLineJob(dummyCommandLine, initialSettings = LocalSettings)
    
    val localResources = makeDummyResources
    
    val termReason = TerminationReason.Memory
    
    val jobDir = dummyJobDir
    
    val runData = RunData(
        job = clj,
        settings = LocalSettings,
        jobStatus = WaitingForOutputs, 
        jobResult = None,
        resourcesOpt = Some(localResources),
        jobDirOpt = Some(jobDir),
        terminationReasonOpt = Some(termReason))
        
    val expected = Execution(
        settings = clj.initialSettings,
        cmd = Some(dummyCommandLine),
        status = Succeeded,
        result = None,
        resources = Option(localResources),
        outputs = Set.empty,
        jobDir = Some(jobDir),
        terminationReason = Some(termReason))
    
    assert(runData.toExecution === expected)
  }
  
  private def makeDummyResources: LocalResources = LocalResources(LocalDateTime.now, LocalDateTime.now)
}
