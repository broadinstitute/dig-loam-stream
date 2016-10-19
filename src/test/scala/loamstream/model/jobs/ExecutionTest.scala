package loamstream.model.jobs

import org.scalatest.FunSuite
import java.nio.file.Paths
import Output.PathOutput
import loamstream.util.TypeBox

/**
 * @author clint
 * Oct 14, 2015
 */
final class ExecutionTest extends FunSuite {
  //scalastyle:off magic.number
  
  private val p0 = Paths.get("foo/bar/baz") 
  private val p1 = Paths.get("nuh")
  
  test("transformOutputs - no outputs") {
    val noOutputs = Execution(JobState.Succeeded, Set.empty)
    
    val transformed = noOutputs.transformOutputs(os => os.map(_ => PathOutput(p0)))
    
    assert(noOutputs === transformed)
  }
  
  test("transformOutputs - some outputs") {
    val hasOutputs = Execution(JobState.Succeeded, Set(p0, p1).map(PathOutput(_)))
    
    val munge: PartialFunction[Output, Output] = {
      case PathOutput(path) => PathOutput(Paths.get(s"${path}123"))
    }
    
    val transformed = hasOutputs.transformOutputs(os => os.map(munge))
    
    assert(hasOutputs.exitState === transformed.exitState)
    assert(transformed.outputs === Set(p0, p1).map(PathOutput(_)).map(munge))
  }
  
  test("isCommandExecution") {
    def assertIsCommandExecution(state: JobState): Unit = {
      def execution(state: JobState) = Execution(state, Set.empty)
      
      assert(execution(state).isCommandExecution)
    }
    
    def assertIsNOTCommandExecution(state: JobState): Unit = {
      def execution(state: JobState) = Execution(state, Set.empty)
      
      assert(!execution(state).isCommandExecution)
    }

    val e = new Exception
    
    import JobState._
    
    assertIsCommandExecution(CommandResult(0))
    assertIsCommandExecution(CommandResult(1))
    assertIsCommandExecution(CommandResult(-1))
    assertIsCommandExecution(CommandResult(42))
    assertIsCommandExecution(CommandInvocationFailure(e))
    
    assertIsNOTCommandExecution(NotStarted)
    assertIsNOTCommandExecution(Running)
    assertIsNOTCommandExecution(Failed)
    assertIsNOTCommandExecution(Succeeded)
    assertIsNOTCommandExecution(Skipped)
    assertIsNOTCommandExecution(Unknown)
    assertIsNOTCommandExecution(FailedWithException(e))
    assertIsNOTCommandExecution(ValueSuccess(123, TypeBox.of[Int]))
  }
  
  //scalastyle:on magic.number
}