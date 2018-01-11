package loamstream.model.execute

import loamstream.TestHelpers
import org.scalatest.FunSuite

import scala.concurrent.Await
import loamstream.model.jobs.{JobStatus, RxMockJob, TestJobs}

import scala.concurrent.duration.Duration
import loamstream.util.ObservableEnrichments
import loamstream.util.Futures
import loamstream.model.jobs.LJob
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.util.PathEnrichments
import loamstream.model.jobs.Output
import loamstream.util.Files
import loamstream.model.jobs.RunData
import scala.concurrent.Future

/**
 * @author clint
 * date: Jun 7, 2016
 */
final class ExecuterHelpersTest extends FunSuite with TestJobs {
  
  import TestHelpers.alwaysRestart
  import TestHelpers.neverRestart
  
  test("statusAndResultFrom") {
    import ExecuterHelpers.statusAndResultFrom
    
    val e = new Exception with scala.util.control.NoStackTrace
    
    val (status, result) = statusAndResultFrom(e)
    
    assert(status === JobStatus.FailedWithException)
    
    assert(result === JobResult.CommandInvocationFailure(e))
  }
  
  test("updateWithException") {
    val execution = Execution(
        env = Environment.Local,
        status = JobStatus.Running,
        result = None,
        outputStreams = None)
        
    val e = new Exception with scala.util.control.NoStackTrace
    
    assert(execution.status === JobStatus.Running)
    assert(execution.result === None)
    
    val updatedExecution = ExecuterHelpers.updateWithException(execution, e)
    
    assert(execution.status === JobStatus.Running)
    assert(execution.result === None)
    
    assert(updatedExecution.status === JobStatus.FailedWithException)
    assert(updatedExecution.result === Some(JobResult.CommandInvocationFailure(e)))
  }

  private def doInThread(f: => Any): Unit = {
    val t = new Thread(new Runnable {
      override def run(): Unit = f
    })
    
    t.setDaemon(true)
    
    t.start()
  }
 
  test("waitForOutputsOnly - some missing outputs") {
    val outDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    import PathEnrichments._
    import ExecuterHelpers.waitForOutputsOnly
    import java.nio.file.Files.exists
    
    val initiallyMissing0 = outDir / "foo.txt"
    val initiallyMissing1 = outDir / "bar.txt"
    val present = outDir / "baz.txt"
    
    val mockJob = MockJob(
        toReturn = JobStatus.Succeeded,
        outputs = Set(
            Output.PathOutput(initiallyMissing0), 
            Output.PathOutput(initiallyMissing1), 
            Output.PathOutput(present)))
        
    Files.writeTo(present)("2")
    
    assert(exists(initiallyMissing0) === false)
    assert(exists(initiallyMissing1) === false)
    assert(exists(present))
    
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val f = waitForOutputsOnly(mockJob, 2.seconds)
    
    assert(exists(initiallyMissing0) === false)
    assert(exists(initiallyMissing1) === false)
    assert(exists(present))
    
    doInThread {
      Thread.sleep(200)
      
      Files.writeTo(initiallyMissing0)("0")
    }
    
    doInThread {
      Thread.sleep(100)
      
      Files.writeTo(initiallyMissing1)("1")
    }
    
    Await.result(f, 3.seconds)
    
    assert(exists(initiallyMissing0))
    assert(exists(initiallyMissing1))
    assert(exists(present))
  }
  
  test("waitForOutputsOnly - no missing outputs") {
    val outDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    import PathEnrichments._
    import ExecuterHelpers.waitForOutputsOnly
    import java.nio.file.Files.exists
    
    val out0 = outDir / "foo.txt"
    val out1 = outDir / "bar.txt"
    
    val mockJob = MockJob(
        toReturn = JobStatus.Succeeded,
        outputs = Set(Output.PathOutput(out0), Output.PathOutput(out1)))
        
    Files.writeTo(out0)("0")
    Files.writeTo(out1)("1")
    
    assert(exists(out0))
    assert(exists(out1))
        
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val f = waitForOutputsOnly(mockJob, 0.seconds)
    
    assert(f.isCompleted)
  }
  
  test("waitForOutputsAndMakeExecution - success, no missing outputs") {
    val outDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    import PathEnrichments._
    import ExecuterHelpers.waitForOutputsAndMakeExecution
    import java.nio.file.Files.exists
    
    val out0 = outDir / "foo.txt"
    val out1 = outDir / "bar.txt"
    
    val mockJob = MockJob(
        toReturn = JobStatus.Succeeded,
        outputs = Set(Output.PathOutput(out0), Output.PathOutput(out1)))
        
    Files.writeTo(out0)("0")
    Files.writeTo(out1)("1")
    
    assert(exists(out0))
    assert(exists(out1))
        
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val runData = RunData(mockJob, JobStatus.Succeeded, Some(JobResult.Success))
    
    val f = waitForOutputsAndMakeExecution(runData, 0.seconds)
    
    val execution = Await.result(f, 10.seconds)
    
    assert(execution === runData.toExecution)
  }
  
  test("waitForOutputsAndMakeExecution - success, some missing outputs") {
    val outDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    import PathEnrichments._
    import ExecuterHelpers.waitForOutputsAndMakeExecution
    import java.nio.file.Files.exists
    
    val out0 = outDir / "foo.txt"
    val out1 = outDir / "bar.txt"
    
    val mockJob = MockJob(
        toReturn = JobStatus.Succeeded,
        outputs = Set(Output.PathOutput(out0), Output.PathOutput(out1)))
        
    Files.writeTo(out0)("0")
    
    assert(exists(out0))
    assert(exists(out1) === false)
        
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val runData = RunData(mockJob, JobStatus.Succeeded, Some(JobResult.Success))
    
    val f = waitForOutputsAndMakeExecution(runData, 5.seconds)
    
    assert(exists(out0))
    assert(exists(out1) === false)
    
    doInThread {
      Thread.sleep(100)
      
      Files.writeTo(out1)("1")
    }
    
    val execution = Await.result(f, 10.seconds)
    
    assert(exists(out0))
    assert(exists(out1))
    
    assert(execution === runData.toExecution)
  }
  
  test("waitForOutputsAndMakeExecution - success, some missing outputs that don't appear in time") {
    val outDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    import PathEnrichments._
    import ExecuterHelpers.waitForOutputsAndMakeExecution
    import java.nio.file.Files.exists
    
    val out0 = outDir / "foo.txt"
    val out1 = outDir / "bar.txt"
    
    val mockJob = MockJob(
        toReturn = JobStatus.Succeeded,
        outputs = Set(Output.PathOutput(out0), Output.PathOutput(out1)))
        
    Files.writeTo(out0)("0")
    
    assert(exists(out0))
    assert(exists(out1) === false)
        
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val runData = RunData(mockJob, JobStatus.Succeeded, Some(JobResult.Success))
    
    //NB: Don't wait for any amount of time
    val f = waitForOutputsAndMakeExecution(runData, 0.seconds)
    
    assert(exists(out0))
    assert(exists(out1) === false)
    
    //NB: Output will appear in 1 second
    doInThread {
      Thread.sleep(1000)
      
      Files.writeTo(out1)("1")
    }
    
    val execution = Await.result(f, 10.seconds)
    
    assert(execution.status === JobStatus.FailedWithException)
  }
  
  test("waitForOutputsAndMakeExecution - failure") {
    import ExecuterHelpers.waitForOutputsAndMakeExecution
    
    val mockJob = MockJob(
        toReturn = JobStatus.FailedPermanently,
        outputs = Set.empty)
        
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val runData = RunData(mockJob, JobStatus.FailedPermanently, Some(JobResult.Failure))
    
    val f = waitForOutputsAndMakeExecution(runData, 5.seconds)
    
    //NB: Outputs aren't considered at all, an already-completed future should be returned
    
    assert(f.isCompleted)
    
    val execution = Await.result(f, 10.seconds)
    
    assert(execution === runData.toExecution)
  }
  
  test("doWaiting - no exception") {
    import ExecuterHelpers.doWaiting
    
    val mockJob = MockJob(JobStatus.Succeeded)
    
    val alreadyComplete = Future.successful(())
    
    def expected = mockJob.toReturn.toExecution
    
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val f = doWaiting(alreadyComplete, mockJob.toReturn.toExecution, throw new Exception)
    
    val execution = Await.result(f, 5.seconds)
    
    assert(execution === expected)
  }
  
  test("doWaiting - exception thrown while making Execution") {
    import ExecuterHelpers.doWaiting
    
    val mockJob = MockJob(JobStatus.Succeeded)
    
    val alreadyComplete = Future.successful(())
    
    def fallbackExecution = Execution.from(mockJob, JobStatus.FailedPermanently)
    
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val f = doWaiting(alreadyComplete, throw new Exception("blerg"), fallbackExecution)
    
    val execution = Await.result(f, 5.seconds)
    
    assert(execution === fallbackExecution)
  }
  
  test("doWaiting - exception thrown while waiting") {
    import ExecuterHelpers.doWaiting
    
    val mockJob = MockJob(JobStatus.Succeeded)
    
    val exception = new Exception with scala.util.control.NoStackTrace
    
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val waitFuture = Future.failed(exception)
    
    def fallbackExecution = Execution.from(mockJob, JobStatus.FailedPermanently)
    
    import scala.concurrent.duration._
    
    val f = doWaiting(waitFuture, mockJob.toReturn.toExecution, fallbackExecution)
    
    val execution = Await.result(f, 5.seconds)
    
    assert(execution.status === JobStatus.FailedWithException)
    assert(execution.result === Some(JobResult.CommandInvocationFailure(exception)))
  }
  
  test("determineFailureStatus") {
    import ExecuterHelpers.determineFailureStatus
    import JobStatus._
    
    def doTest(failureStatus: JobStatus): Unit = {
      val job = MockJob(NotStarted)
      
      assert(job.status === NotStarted)
      
      assert(determineFailureStatus(alwaysRestart, failureStatus, job) === failureStatus)
      
      assert(determineFailureStatus(neverRestart, failureStatus, job) === FailedPermanently)
      
      assert(job.status === NotStarted)
    }
    
    doTest(Failed)
    doTest(FailedWithException)
    doTest(Terminated)
  }
  
  test("flattenTree") {
    import ExecuterHelpers.flattenTree
    
    val noDeps0 = RxMockJob("noDeps")
    
    assert(flattenTree(Set(noDeps0)) == Set(noDeps0))
    
    val middle0 = RxMockJob("middle", Set(noDeps0))
    
    assert(flattenTree(Set(middle0)) == Set(middle0, noDeps0))
    
    val root0 = RxMockJob("root", Set(middle0))
    
    assert(flattenTree(Set(root0)) == Set(root0, middle0, noDeps0))
    
    val noDeps1 = RxMockJob("noDeps1")
    val middle1 = RxMockJob("middle1", Set(noDeps1))
    val root1 = RxMockJob("root1", Set(middle1))
    
    assert(flattenTree(Set(root0, root1)) == Set(root0, middle0, noDeps0, root1, middle1, noDeps1))
  }
  
  test("noFailures() and anyFailures()") {
    import ExecuterHelpers.{noFailures,anyFailures}

    assert(noFailures(Map.empty) === true)
    assert(anyFailures(Map.empty) === false)

    val allSuccesses = Map( two0 -> two0Success,
                            two1 -> two1Success,
                            twoPlusTwo -> twoPlusTwoSuccess,
                            plusOne -> plusOneSuccess).mapValues(TestHelpers.executionFrom(_))
      
    assert(noFailures(allSuccesses) === true)
    assert(anyFailures(allSuccesses) === false)
    
    val allFailures = Map(
                          two0 -> JobStatus.Failed,
                          two1 -> JobStatus.Failed,
                          twoPlusTwo -> JobStatus.Failed,
                          plusOne -> JobStatus.Failed).mapValues(TestHelpers.executionFrom(_))
      
    assert(noFailures(allFailures) === false)
    assert(anyFailures(allFailures) === true)
    
    val someFailures = Map(
                            two0 -> two0Success,
                            two1 -> JobStatus.Failed,
                            twoPlusTwo -> twoPlusTwoSuccess,
                            plusOne -> JobStatus.Failed).mapValues(TestHelpers.executionFrom(_))
      
    assert(noFailures(someFailures) === false)
    assert(anyFailures(someFailures) === true)
  }
}
