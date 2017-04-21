package loamstream.model.jobs

import org.scalatest.FunSuite
import java.nio.file.Paths

import loamstream.model.execute._
import loamstream.util.TypeBox
import loamstream.TestHelpers
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.jobs.JobResult.CommandResult

/**
 * @author clint
 * Oct 14, 2015
 */
final class ExecutionTest extends FunSuite with ProvidesEnvAndResources {
  //scalastyle:off magic.number

  private val p0 = Paths.get("foo/bar/baz")
  private val p1 = Paths.get("nuh")
  
  test("from(LJob, JobStatus, JobResult)") {
    import TestHelpers.path
    
    val result0 = CommandResult(0)
    val status0 = JobStatus.fromExitCode(result0.exitCode)
    val status1 = JobStatus.Succeeded
    
    val job0 = CommandLineStringJob("foo", path("."), ExecutionEnvironment.Uger)
    val job1 = MockJob(status1)
    
    val e0 = Execution.from(job0, status0, Option(result0))
    val e1 = Execution.from(job1, status1)
    
    assert(e0.cmd === Some("foo"))
    assert(e0.env.isUger)
    assert(e0.result === Some(result0))
    assert(e0.outputs.isEmpty)
    //TODO: Check settings field once it's no longer a placeholder 
    
    assert(e1.cmd === None)
    assert(e1.env.isLocal)
    assert(e1.status === status1)
    assert(e1.result === None)
    assert(e1.outputs.isEmpty)
    //TODO: Check settings field once it's no longer a placeholder
  }

  test("isCommandExecution") {
    def assertIsCommandExecution(result: JobResult, cmd: Option[String] = Option(mockCmd)): Unit = {
      val execution = Execution(mockEnv, cmd, mockSettings, result, Set.empty[OutputRecord])
      
      assert(execution.isCommandExecution)
    }
    
    def assertIsNOTCommandExecution(result: JobResult, cmd: Option[String] = Option(mockCmd)): Unit = {
      val execution = Execution(mockEnv, cmd, mockSettings, result, Set.empty[OutputRecord])
      
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
    assertIsNOTCommandExecution(ValueSuccess(123, TypeBox.of[Int]))

    assertIsNOTCommandExecution(Success, None)
    assertIsNOTCommandExecution(Failure, None)
    assertIsNOTCommandExecution(FailureWithException(e), None)
    assertIsNOTCommandExecution(ValueSuccess(123, TypeBox.of[Int]), None)
  }

  //scalastyle:on magic.number
}
