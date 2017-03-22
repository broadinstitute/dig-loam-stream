package loamstream.model.jobs

import org.scalatest.FunSuite
import java.nio.file.Paths

import Output.PathOutput
import loamstream.model.execute._
import loamstream.model.execute.ExecutionEnvironment.Local
import loamstream.util.TypeBox
import loamstream.model.execute.Resources.LocalResources
import loamstream.TestHelpers
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.JobResult.Succeeded

/**
 * @author clint
 * Oct 14, 2015
 */
final class ExecutionTest extends FunSuite with ProvidesEnvAndResources {
  //scalastyle:off magic.number

  private val p0 = Paths.get("foo/bar/baz")
  private val p1 = Paths.get("nuh")
  
  test("from(LJob, JobState)") {
    import TestHelpers.path
    
    val state0 = CommandResult(0, Some(ProvidesEnvAndResources.mockResources))
    val state1 = Succeeded
    
    val job0 = CommandLineStringJob("foo", path("."), ExecutionEnvironment.Uger)
    val job1 = MockJob(state1)
    
    val e0 = Execution.from(job0, state0)
    val e1 = Execution.from(job1, state1)
    
    assert(e0.cmd === Some("foo"))
    assert(e0.env.isUger)
    assert(e0.result === state0)
    assert(e0.outputs.isEmpty)
    //TODO: Check settings field once it's no longer a placeholder 
    
    assert(e1.cmd === None)
    assert(e1.env.isLocal)
    assert(e1.result === state1)
    assert(e1.outputs.isEmpty)
    //TODO: Check settings field once it's no longer a placeholder
  }
  
  test("transformOutputs - no outputs") {
    val noOutputs = Execution(mockEnv, Option(mockCmd), mockSettings, JobResult.Succeeded, Set.empty[OutputRecord])
    
    val transformed = noOutputs.transformOutputs(os => os.map(_ => PathOutput(p0).toOutputRecord))
    
    assert(noOutputs === transformed)
  }
  
  test("transformOutputs - some outputs") {
    val hasOutputs = Execution(mockEnv, Option(mockCmd), mockSettings, 
        JobResult.Succeeded, Set(p0, p1).map(PathOutput(_).toOutputRecord))
    
    val munge: OutputRecord => OutputRecord = rec => OutputRecord(s"${rec.loc}123", None, None)

    val transformed = hasOutputs.transformOutputs(os => os.map(munge))
    
    assert(hasOutputs.result === transformed.result)
    assert(transformed.outputs === Set(p0, p1).map(PathOutput(_).toOutputRecord).map(munge))
  }
  
  test("isCommandExecution") {
    def assertIsCommandExecution(state: JobResult, cmd: Option[String] = Option(mockCmd)): Unit = {
      val execution = Execution(mockEnv, cmd, mockSettings, state, Set.empty[OutputRecord])
      
      assert(execution.isCommandExecution)
    }
    
    def assertIsNOTCommandExecution(state: JobResult, cmd: Option[String] = Option(mockCmd)): Unit = {
      val execution = Execution(mockEnv, cmd, mockSettings, state, Set.empty[OutputRecord])
      
      assert(!execution.isCommandExecution)
    }

    val e = new Exception
    
    import JobResult._
    
    assertIsCommandExecution(CommandResult(0, Some(TestHelpers.localResources)) )
    assertIsCommandExecution(CommandResult(1, Some(TestHelpers.localResources)))
    assertIsCommandExecution(CommandResult(-1, Some(TestHelpers.localResources)))
    assertIsCommandExecution(CommandResult(42, Some(TestHelpers.localResources)))
    
    assertIsCommandExecution(CommandResult(0, None))
    assertIsCommandExecution(CommandResult(1, None))
    assertIsCommandExecution(CommandResult(-1, None))
    assertIsCommandExecution(CommandResult(42, None))
    
    assertIsCommandExecution(CommandInvocationFailure(e))
    
    assertIsNOTCommandExecution(CommandResult(0, Some(TestHelpers.localResources)), None )
    assertIsNOTCommandExecution(CommandResult(1, Some(TestHelpers.localResources)), None)
    assertIsNOTCommandExecution(CommandResult(-1, Some(TestHelpers.localResources)), None)
    assertIsNOTCommandExecution(CommandResult(42, Some(TestHelpers.localResources)), None)
    
    assertIsNOTCommandExecution(CommandResult(0, None), None)
    assertIsNOTCommandExecution(CommandResult(1, None), None)
    assertIsNOTCommandExecution(CommandResult(-1, None), None)
    assertIsNOTCommandExecution(CommandResult(42, None), None)
    
    assertIsNOTCommandExecution(CommandInvocationFailure(e), None)
    
    assertIsNOTCommandExecution(NotStarted)
    assertIsNOTCommandExecution(Running)
    assertIsNOTCommandExecution(Failed())
    assertIsNOTCommandExecution(Succeeded)
    assertIsNOTCommandExecution(Skipped)
    assertIsNOTCommandExecution(Unknown)
    assertIsNOTCommandExecution(FailedWithException(e))
    assertIsNOTCommandExecution(ValueSuccess(123, TypeBox.of[Int]))
    
    assertIsNOTCommandExecution(NotStarted, None)
    assertIsNOTCommandExecution(Running, None)
    assertIsNOTCommandExecution(Failed(), None)
    assertIsNOTCommandExecution(Succeeded, None)
    assertIsNOTCommandExecution(Skipped, None)
    assertIsNOTCommandExecution(Unknown, None)
    assertIsNOTCommandExecution(FailedWithException(e), None)
    assertIsNOTCommandExecution(ValueSuccess(123, TypeBox.of[Int]), None)
  }
  
  //scalastyle:on magic.number
}
