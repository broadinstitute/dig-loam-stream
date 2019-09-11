package loamstream.model.jobs

import java.nio.file.Paths
import java.time.Instant

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.drm.Queue
import loamstream.drm.uger.UgerDefaults
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.GoogleSettings
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.ProvidesEnvAndResources
import loamstream.model.execute.Resources
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.util.TypeBox
import loamstream.model.execute.UgerDrmSettings
import loamstream.model.execute.LsfDrmSettings
import loamstream.model.execute.Settings
import loamstream.googlecloud.ClusterConfig


/**
 * @author clint
 * Oct 14, 2015
 */
final class ExecutionTest extends FunSuite with ProvidesEnvAndResources {
  private val p0 = Paths.get("foo/bar/baz")
  private val p1 = Paths.get("nuh")
  
  import loamstream.TestHelpers.dummyJobDir
  
  test("WithCommandResult") {
    val commandResult0 = CommandResult(0)
    val commandResult1 = CommandResult(42)
    
    val job = MockJob(JobStatus.Succeeded)
    
    def withResult(rOpt: Option[JobResult]): Execution = {
      Execution.from(job = job, status = job.toReturn.jobStatus, result = rOpt, terminationReason = None)
    }
    
    def doTest(rOpt: Option[JobResult], expected: Option[JobResult]): Unit = {
      assert(Execution.WithCommandResult.unapply(withResult(rOpt)) === expected)
    }
    
    val ex = new Exception with scala.util.control.NoStackTrace
    
    doTest(None, None)
    doTest(Some(JobResult.CommandInvocationFailure(ex)), None)
    doTest(Some(JobResult.FailureWithException(ex)), None)
    doTest(Some(JobResult.Success), None)
    doTest(Some(JobResult.Failure), None)
    
    doTest(Some(commandResult0), Some(commandResult0))
    doTest(Some(commandResult1), Some(commandResult1))
  }
  
  test("WithCommandInvocationFailure") {
    val commandResult0 = CommandResult(0)
    val commandResult1 = CommandResult(42)
    
    val job = MockJob(JobStatus.Succeeded)
    
    def withResult(rOpt: Option[JobResult]): Execution = {
      Execution.from(job = job, status = job.toReturn.jobStatus, result = rOpt, terminationReason = None)
    }
    
    def doTest(rOpt: Option[JobResult], expected: Option[JobResult]): Unit = {
      assert(Execution.WithCommandInvocationFailure.unapply(withResult(rOpt)) === expected)
    }
    
    doTest(None, None)
    
    val ex = new Exception with scala.util.control.NoStackTrace
    
    doTest(Some(JobResult.FailureWithException(ex)), None)
    doTest(Some(JobResult.Success), None)
    doTest(Some(JobResult.Failure), None)
    doTest(Some(commandResult0), None)
    doTest(Some(commandResult1), None)
    
    val cif = JobResult.CommandInvocationFailure(ex) 
    
    doTest(Some(cif), Some(cif))
  }
  
  test("WithThrowable") {
    val commandResult0 = CommandResult(0)
    val commandResult1 = CommandResult(42)
    
    val job = MockJob(JobStatus.Succeeded)
    
    def withResult(rOpt: Option[JobResult]): Execution = {
      Execution.from(job = job, status = job.toReturn.jobStatus, result = rOpt, terminationReason = None)
    }
    
    def doTest(rOpt: Option[JobResult], expected: Option[Throwable]): Unit = {
      assert(Execution.WithThrowable.unapply(withResult(rOpt)) === expected)
    }
    
    doTest(None, None)
    
    val ex = new Exception with scala.util.control.NoStackTrace
    
    doTest(Some(JobResult.FailureWithException(ex)), Some(ex))
    doTest(Some(JobResult.CommandInvocationFailure(ex)), Some(ex))
    doTest(Some(JobResult.Success), None)
    doTest(Some(JobResult.Failure), None)
    doTest(Some(commandResult0), None)
    doTest(Some(commandResult1), None)
  }
  
  test("guards") {
    val localSettings = LocalSettings
    val ugerSettings = {
      UgerDrmSettings(Cpus(8), Memory.inGb(4), UgerDefaults.maxRunTime, Option(UgerDefaults.queue), None)
    }
    val lsfSettings = {
      LsfDrmSettings(Cpus(8), Memory.inGb(4), UgerDefaults.maxRunTime, Option(UgerDefaults.queue), None)
    }
    val googleSettings = GoogleSettings("some-cluster", ClusterConfig.default)

    val localResources = LocalResources(Instant.ofEpochMilli(123), Instant.ofEpochMilli(456))

    val ugerResources = UgerResources(
        Memory.inGb(2.1),
        CpuTime.inSeconds(12.34),
        Some("nodeName"),
        Some(Queue("broad")),
        Instant.ofEpochMilli(64532),
        Instant.ofEpochMilli(9345345))
        
    val lsfResources = LsfResources(
        Memory.inGb(1.2),
        CpuTime.inSeconds(34.21),
        Some("another-node"),
        Some(Queue("ebi")),
        Instant.ofEpochMilli(12345),
        Instant.ofEpochMilli(12346))

    val googleResources = GoogleResources(
        "clusterName",
        Instant.ofEpochMilli(1), 
        Instant.ofEpochMilli(72345))
      
    def doTest(settings: Settings, resources: Option[Resources], shouldThrow: Boolean): Unit = {
      type WrapperFn = (=> Any) => Unit
    
      val justRun: WrapperFn = { block => 
        block 
        ()
      }
      
      val interceptException: WrapperFn = { block =>
        intercept[Exception] {
          block
        }
      }
    
      val runOrInterceptExpectedExceptions: WrapperFn = if(shouldThrow) interceptException else justRun
    
      runOrInterceptExpectedExceptions {
        val result = CommandResult(0)
      
        Execution(
          envType = settings.envType,
          settings = Option(settings),
          cmd = Option(mockCmd),
          status = result.toJobStatus,
          result = Option(result),
          resources = resources,
          jobDir = Option(dummyJobDir), 
          outputs = Set.empty,
          terminationReason = None)
      }
    }
    
    doTest(localSettings, Some(localResources), shouldThrow = false)
    doTest(ugerSettings, Some(ugerResources), shouldThrow = false)
    doTest(lsfSettings, Some(lsfResources), shouldThrow = false)
    doTest(googleSettings, Some(googleResources), shouldThrow = false)
    
    doTest(localSettings, None, shouldThrow = false)
    doTest(ugerSettings, None, shouldThrow = false)
    doTest(lsfSettings, None, shouldThrow = false)
    doTest(googleSettings, None, shouldThrow = false)
    
    doTest(localSettings, Some(googleResources), shouldThrow = true)
    doTest(ugerSettings, Some(localResources), shouldThrow = true)
    doTest(lsfSettings, Some(ugerResources), shouldThrow = true)
    doTest(googleSettings, Some(lsfResources), shouldThrow = true)
  }
  
  test("from(LJob, JobStatus, JobResult)") {
    import TestHelpers.path
    
    val result0 = CommandResult(0)
    val status0 = JobStatus.fromExitCode(result0.exitCode)
    val status1 = JobStatus.Succeeded
    
    val job0 = CommandLineJob("foo", path("."), TestHelpers.defaultUgerSettings)
    val job1 = MockJob(status1)
    
    val jobDirOpt = Some(dummyJobDir)
    
    val e0 = Execution.from(job0, status0, Option(result0), jobDirOpt, terminationReason = None)
    val e1 = Execution.from(job1, status1, terminationReason = None)
    
    assert(e0.cmd === Some("foo"))
    assert(e0.settings.get.isUger)
    assert(e0.result === Some(result0))
    assert(e0.outputs === Set.empty)
    assert(e0.jobDir === jobDirOpt)
    //TODO: Check settings field once it's no longer a placeholder 
    
    assert(e1.cmd === None)
    assert(e1.settings.get.isLocal)
    assert(e1.status === status1)
    assert(e1.result === None)
    assert(e1.outputs === Set.empty)
    assert(e1.jobDir === None)
    //TODO: Check settings field once it's no longer a placeholder
  }

  test("isCommandExecution") {
    def assertIsCommandExecution(result: JobResult, cmd: Option[String] = Option(mockCmd)): Unit = {
      val execution = Execution(
          envType = mockUgerSettings.envType,
          settings = Option(mockUgerSettings),
          cmd = cmd, 
          status = result.toJobStatus,
          result = Option(result),
          resources = None,
          jobDir = Option(dummyJobDir),
          outputs = Set.empty,
          terminationReason = None)
      
      assert(execution.isCommandExecution)
    }
    
    def assertIsNOTCommandExecution(result: JobResult, cmd: Option[String] = Option(mockCmd)): Unit = {
      val execution = Execution(
          envType = mockUgerSettings.envType,
          settings = Option(mockUgerSettings),
          cmd = cmd, 
          status = result.toJobStatus,
          result = Option(result),
          resources = None,
          jobDir = Option(dummyJobDir),
          outputs = Set.empty,
          terminationReason = None)
      
      assert(!execution.isCommandExecution)
    }

    val e = new Exception
    
    import JobResult._
    
    assertIsCommandExecution(CommandResult(0))
    assertIsCommandExecution(CommandResult(1))
    assertIsCommandExecution(CommandResult(-1))
    assertIsCommandExecution(CommandResult(42))

    assertIsCommandExecution(CommandInvocationFailure(e))

    assertIsNOTCommandExecution(CommandResult(0), None)
    assertIsNOTCommandExecution(CommandResult(1), None)
    assertIsNOTCommandExecution(CommandResult(-1), None)
    assertIsNOTCommandExecution(CommandResult(42), None)

    assertIsNOTCommandExecution(CommandInvocationFailure(e), None)
    
    assertIsNOTCommandExecution(Success)
    assertIsNOTCommandExecution(Failure)
    assertIsNOTCommandExecution(FailureWithException(e))

    assertIsNOTCommandExecution(Success, None)
    assertIsNOTCommandExecution(Failure, None)
    assertIsNOTCommandExecution(FailureWithException(e), None)
  }
}
