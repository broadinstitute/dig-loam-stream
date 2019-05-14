package loamstream.model.jobs

import java.nio.file.Paths
import java.time.Instant

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.drm.Queue
import loamstream.drm.uger.UgerDefaults
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.Environment
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


/**
 * @author clint
 * Oct 14, 2015
 */
final class ExecutionTest extends FunSuite with ProvidesEnvAndResources {
  private val p0 = Paths.get("foo/bar/baz")
  private val p1 = Paths.get("nuh")
  
  import loamstream.TestHelpers.dummyOutputStreams
  
  test("guards") {
    val localSettings = LocalSettings
    val ugerSettings = {
      UgerDrmSettings(Cpus(8), Memory.inGb(4), UgerDefaults.maxRunTime, Option(UgerDefaults.queue), None)
    }
    val lsfSettings = {
      LsfDrmSettings(Cpus(8), Memory.inGb(4), UgerDefaults.maxRunTime, Option(UgerDefaults.queue), None)
    }
    val googleSettings = GoogleSettings("some-cluster")

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
          settings = settings,
          cmd = Option(mockCmd),
          status = result.toJobStatus,
          result = Option(result),
          resources = resources,
          outputStreams = Option(dummyOutputStreams), 
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
    
    val outputStreamsOpt = Some(dummyOutputStreams)
    
    val e0 = Execution.from(job0, status0, Option(result0), outputStreamsOpt, terminationReason = None)
    val e1 = Execution.from(job1, status1, terminationReason = None)
    
    assert(e0.cmd === Some("foo"))
    assert(e0.settings.isUger)
    assert(e0.result === Some(result0))
    assert(e0.outputs === Set.empty)
    assert(e0.outputStreams === outputStreamsOpt)
    //TODO: Check settings field once it's no longer a placeholder 
    
    assert(e1.cmd === None)
    assert(e1.settings.isLocal)
    assert(e1.status === status1)
    assert(e1.result === None)
    assert(e1.outputs === Set.empty)
    assert(e1.outputStreams === None)
    //TODO: Check settings field once it's no longer a placeholder
  }

  test("isCommandExecution") {
    def assertIsCommandExecution(result: JobResult, cmd: Option[String] = Option(mockCmd)): Unit = {
      val execution = Execution(
          settings = mockUgerSettings,
          cmd = cmd, 
          status = result.toJobStatus,
          result = Option(result),
          resources = None,
          outputStreams = Option(dummyOutputStreams),
          outputs = Set.empty,
          terminationReason = None)
      
      assert(execution.isCommandExecution)
    }
    
    def assertIsNOTCommandExecution(result: JobResult, cmd: Option[String] = Option(mockCmd)): Unit = {
      val execution = Execution(
          settings = mockUgerSettings,
          cmd = cmd, 
          status = result.toJobStatus,
          result = Option(result),
          resources = None,
          outputStreams = Option(dummyOutputStreams),
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
