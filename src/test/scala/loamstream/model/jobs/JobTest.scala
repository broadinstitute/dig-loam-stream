package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.util.Futures
import loamstream.util.ObservableEnrichments
import scala.concurrent.ExecutionContext

/**
 * @author clint
 * date: May 27, 2016
 */
final class JobTest extends FunSuite with TestJobs {
  
  //scalastyle:off magic.number
  
  import JobState._
  
  test("isRunnable - single job") {
    val singleJob  = MockJob(Succeeded, "single job", Set.empty, Set.empty, 0)

    assert(singleJob.state == NotStarted)
    
    assert(singleJob.isRunnable)
  }
  
  test("isRunnable - linear 2 job pipeline") {
    val dep = MockJob(Succeeded, "dep", Set.empty, Set.empty, 0)
    
    val root = MockJob(Succeeded, "root", Set(dep), Set.empty, 0)

    assert(dep.state ==NotStarted)
    assert(root.state == NotStarted)
    
    assert(dep.isRunnable)
    assert(root.isRunnable === false)
  }
  
  private def jobWithState(state: JobState, inputStates: Set[JobState] = Set.empty): MockJob = {
    val job = MockJob(state, inputs = inputStates.map(jobWithState(_)))
    
    job.updateAndEmitJobState(state)
    
    job
  }
  
  test("isFinished") {
    val e = new Exception
    
    //No deps, success
    assert(jobWithState(Succeeded).isFinished)
    assert(jobWithState(CommandResult(0)).isFinished)
    //No deps, failure
    assert(jobWithState(Failed).isFinished)
    assert(jobWithState(FailedWithException(e)).isFinished)
    assert(jobWithState(CommandInvocationFailure(e)).isFinished)
    assert(jobWithState(CommandResult(42)).isFinished)
    //No deps, not finished
    assert(jobWithState(NotStarted).isFinished === false)
    assert(jobWithState(Running).isFinished === false)

    def doTestWithSuccessfulInputStates(successfulInputStates: Set[JobState]): Unit = {
      //All deps succeeded, success
      assert(jobWithState(Succeeded, successfulInputStates).isFinished)
      assert(jobWithState(CommandResult(0), successfulInputStates).isFinished)
      assert(jobWithState(Succeeded, successfulInputStates).isFinished)
      assert(jobWithState(CommandResult(0), successfulInputStates).isFinished)
      //All deps succeeded, failure
      assert(jobWithState(Failed, successfulInputStates).isFinished)
      assert(jobWithState(FailedWithException(e), successfulInputStates).isFinished)
      assert(jobWithState(CommandInvocationFailure(e), successfulInputStates).isFinished)
      assert(jobWithState(CommandResult(42), successfulInputStates).isFinished)
      //All deps succeeded, not finished
      assert(jobWithState(NotStarted, successfulInputStates).isFinished === false)
      assert(jobWithState(Running, successfulInputStates).isFinished === false)
    }
    
    doTestWithSuccessfulInputStates(Set(Succeeded, CommandResult(0)))
    doTestWithSuccessfulInputStates(Set(Succeeded, Succeeded, Succeeded))
    doTestWithSuccessfulInputStates(Set(Succeeded))
    doTestWithSuccessfulInputStates(Set(Succeeded, Skipped, Succeeded))
    doTestWithSuccessfulInputStates(Set(Skipped))
    doTestWithSuccessfulInputStates(Set(Skipped, Skipped, Skipped))
    doTestWithSuccessfulInputStates(Set(Skipped, CommandResult(0), Skipped))
    
    def doTestWithFailedInputStates(failedInputStates: Set[JobState]): Unit = {
      //All deps succeeded, success
      assert(jobWithState(Succeeded, failedInputStates).isFinished)
      assert(jobWithState(CommandResult(0), failedInputStates).isFinished)
      assert(jobWithState(Succeeded, failedInputStates).isFinished)
      assert(jobWithState(CommandResult(0), failedInputStates).isFinished)
      //All deps succeeded, failure
      assert(jobWithState(Failed, failedInputStates).isFinished)
      assert(jobWithState(FailedWithException(e), failedInputStates).isFinished)
      assert(jobWithState(CommandInvocationFailure(e), failedInputStates).isFinished)
      assert(jobWithState(CommandResult(42), failedInputStates).isFinished)
      //All deps succeeded, not finished
      assert(jobWithState(NotStarted, failedInputStates).isFinished)
      assert(jobWithState(Running, failedInputStates).isFinished)
    }
    
    doTestWithFailedInputStates(Set(Succeeded, CommandResult(42)))
    doTestWithFailedInputStates(Set(CommandResult(42)))
    doTestWithFailedInputStates(Set(Failed, Failed))
    doTestWithFailedInputStates(Set(CommandInvocationFailure(e)))
    doTestWithFailedInputStates(Set(FailedWithException(e)))
  }
  
  test("inputsFinished") {
    val e = new Exception
    
    //no inputs
    assert(jobWithState(Succeeded).inputsFinished)
    assert(jobWithState(Failed).inputsFinished)
    assert(jobWithState(Skipped).inputsFinished)
    assert(jobWithState(CommandResult(0)).inputsFinished)
    assert(jobWithState(CommandResult(42)).inputsFinished)
    assert(jobWithState(CommandInvocationFailure(e)).inputsFinished)
    assert(jobWithState(FailedWithException(e)).inputsFinished)
    assert(jobWithState(NotStarted).inputsFinished)
    assert(jobWithState(Running).inputsFinished)
    
    //some inputs
    
    def doTestWithFinishedInputStates(finishedInputStates: Set[JobState]): Unit = {
      //All deps succeeded, success
      assert(jobWithState(Succeeded, finishedInputStates).inputsFinished)
      assert(jobWithState(CommandResult(0), finishedInputStates).inputsFinished)
      assert(jobWithState(Succeeded, finishedInputStates).inputsFinished)
      assert(jobWithState(CommandResult(0), finishedInputStates).inputsFinished)
      //All deps succeeded, failure
      assert(jobWithState(Failed, finishedInputStates).inputsFinished)
      assert(jobWithState(FailedWithException(e), finishedInputStates).inputsFinished)
      assert(jobWithState(CommandInvocationFailure(e), finishedInputStates).inputsFinished)
      assert(jobWithState(CommandResult(42), finishedInputStates).inputsFinished)
      //All deps succeeded, not finished
      assert(jobWithState(NotStarted, finishedInputStates).inputsFinished)
      assert(jobWithState(Running, finishedInputStates).inputsFinished)
    }
    
    doTestWithFinishedInputStates(Set(Succeeded, Failed))
    doTestWithFinishedInputStates(Set(Failed))
    doTestWithFinishedInputStates(Set(Succeeded))
    doTestWithFinishedInputStates(Set(Skipped, Failed))
    doTestWithFinishedInputStates(Set(Skipped, CommandResult(0)))
    
    def doTestWithUnfinishedInputStates(unfinishedInputStates: Set[JobState]): Unit = {
      //All deps succeeded, success
      assert(jobWithState(Succeeded, unfinishedInputStates).inputsFinished === false)
      assert(jobWithState(CommandResult(0), unfinishedInputStates).inputsFinished === false)
      assert(jobWithState(Succeeded, unfinishedInputStates).inputsFinished === false)
      assert(jobWithState(CommandResult(0), unfinishedInputStates).inputsFinished === false)
      //All deps succeeded, failure
      assert(jobWithState(Failed, unfinishedInputStates).inputsFinished === false)
      assert(jobWithState(FailedWithException(e), unfinishedInputStates).inputsFinished === false)
      assert(jobWithState(CommandInvocationFailure(e), unfinishedInputStates).inputsFinished === false)
      assert(jobWithState(CommandResult(42), unfinishedInputStates).inputsFinished === false)
      //All deps succeeded, not finished
      assert(jobWithState(NotStarted, unfinishedInputStates).inputsFinished === false)
      assert(jobWithState(Running, unfinishedInputStates).inputsFinished === false)
    }
    
    doTestWithUnfinishedInputStates(Set(Running))
    doTestWithUnfinishedInputStates(Set(NotStarted))
    doTestWithUnfinishedInputStates(Set(NotStarted, NotStarted, NotStarted))
    doTestWithUnfinishedInputStates(Set(Running, Running, Running))
    doTestWithUnfinishedInputStates(Set(NotStarted, Running, NotStarted))
  }
  
  test("inputsSuccessful") {
    val e = new Exception
    
    //no inputs
    assert(jobWithState(Succeeded).inputsSuccessful)
    assert(jobWithState(Failed).inputsSuccessful)
    assert(jobWithState(Skipped).inputsSuccessful)
    assert(jobWithState(CommandResult(0)).inputsSuccessful)
    assert(jobWithState(CommandResult(42)).inputsSuccessful)
    assert(jobWithState(CommandInvocationFailure(e)).inputsSuccessful)
    assert(jobWithState(FailedWithException(e)).inputsSuccessful)
    assert(jobWithState(NotStarted).inputsSuccessful)
    assert(jobWithState(Running).inputsSuccessful)
    
    def doTestWithSuccessfulInputs(inputStates: Set[JobState]): Unit = {
      assert(jobWithState(Succeeded, inputStates).inputsSuccessful)
      assert(jobWithState(Failed, inputStates).inputsSuccessful)
      assert(jobWithState(Skipped, inputStates).inputsSuccessful)
      assert(jobWithState(CommandResult(0), inputStates).inputsSuccessful)
      assert(jobWithState(CommandResult(42), inputStates).inputsSuccessful)
      assert(jobWithState(CommandInvocationFailure(e), inputStates).inputsSuccessful)
      assert(jobWithState(FailedWithException(e), inputStates).inputsSuccessful)
      assert(jobWithState(NotStarted, inputStates).inputsSuccessful)
      assert(jobWithState(Running, inputStates).inputsSuccessful)
    }
    
    doTestWithSuccessfulInputs(Set(Succeeded, Succeeded))
    doTestWithSuccessfulInputs(Set(Succeeded, Skipped))
    doTestWithSuccessfulInputs(Set(Skipped, Skipped))
    doTestWithSuccessfulInputs(Set(CommandResult(0)))
    
    def doTestWithFailedInputs(inputStates: Set[JobState]): Unit = {
      assert(jobWithState(Succeeded, inputStates).inputsSuccessful === false)
      assert(jobWithState(Failed, inputStates).inputsSuccessful === false)
      assert(jobWithState(Skipped, inputStates).inputsSuccessful === false)
      assert(jobWithState(CommandResult(0), inputStates).inputsSuccessful === false)
      assert(jobWithState(CommandResult(42), inputStates).inputsSuccessful === false)
      assert(jobWithState(CommandInvocationFailure(e), inputStates).inputsSuccessful === false)
      assert(jobWithState(FailedWithException(e), inputStates).inputsSuccessful === false)
      assert(jobWithState(NotStarted, inputStates).inputsSuccessful === false)
      assert(jobWithState(Running, inputStates).inputsSuccessful === false)
    }
    
    doTestWithFailedInputs(Set(Failed))
    doTestWithFailedInputs(Set(CommandResult(42)))
    doTestWithFailedInputs(Set(FailedWithException(e)))
    doTestWithFailedInputs(Set(CommandInvocationFailure(e)))
    doTestWithFailedInputs(Set(Failed, Failed))
    doTestWithFailedInputs(Set(CommandResult(42), Failed))
    doTestWithFailedInputs(Set(FailedWithException(e), Failed))
    doTestWithFailedInputs(Set(CommandInvocationFailure(e), Failed))

    //Mix in some successes
    doTestWithFailedInputs(Set(Succeeded, Failed, Succeeded))
    doTestWithFailedInputs(Set(Succeeded, CommandResult(42), Succeeded))
    doTestWithFailedInputs(Set(Succeeded, FailedWithException(e), Succeeded))
    doTestWithFailedInputs(Set(Succeeded, CommandInvocationFailure(e), Succeeded))
  }
  
  test("execute") {
    val job = MockJob(CommandResult(42))
    
    import Futures.waitFor
    import ObservableEnrichments._
    
    val states = job.states.until(_.isFinished).to[Seq].firstAsFuture
    
    job.execute(ExecutionContext.global)
    
    assert(waitFor(states) === Seq(NotStarted, Running, CommandResult(42)))
  }
  
  //scalastyle:on magic.number
}