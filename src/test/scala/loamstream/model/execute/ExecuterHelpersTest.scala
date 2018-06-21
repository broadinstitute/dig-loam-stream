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
import loamstream.util.FileMonitor
import loamstream.LoamFunSuite
import java.nio.file.Path

/**
 * @author clint
 * date: Jun 7, 2016
 */
final class ExecuterHelpersTest extends LoamFunSuite with TestJobs {
  
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
  
  private def doInThreadAfter(millis: Long)(f: => Any): Unit = doInThread {  
    Thread.sleep(millis)
    
    f
  }
 
  test("waitForOutputsOnly - some missing outputs") { 
    
    import PathEnrichments.PathHelpers
    import ExecuterHelpers.waitForOutputsOnly
    import java.nio.file.Files.exists
    import TestHelpers.path
    
    def doTest(
        makeInitiallyMissing0: Path => Path,
        makeInitiallyMissing1: Path => Path,
        makePresent: Path => Path,
        getLocations: Path => Locations[Path]): Unit = TestHelpers.withWorkDir(getClass.getSimpleName) { outDir =>
          
      val locations = getLocations(outDir)
          
      val initiallyMissing0 = makeInitiallyMissing0(outDir)
      val initiallyMissing1 = makeInitiallyMissing1(outDir)
      val present = makePresent(outDir)
      
      val initiallyMissingOutput0 = Output.PathOutput(initiallyMissing0, locations)
      val initiallyMissingOutput1 = Output.PathOutput(initiallyMissing1, locations)
      val presentOutput = Output.PathOutput(present, locations)
      
      val mockJob = MockJob(
          toReturn = JobStatus.Succeeded,
          outputs = Set(initiallyMissingOutput0, initiallyMissingOutput1, presentOutput))
          
      Files.writeTo(presentOutput.pathInHost)("2")
      
      assert(exists(initiallyMissingOutput0.pathInHost) === false)
      assert(exists(initiallyMissingOutput1.pathInHost) === false)
      assert(exists(presentOutput.pathInHost))
      
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global
      
      withFileMonitor(new FileMonitor(10.0, 2.seconds)) { fileMonitor =>
        val f = waitForOutputsOnly(mockJob, fileMonitor)
        
        assert(exists(initiallyMissingOutput0.pathInHost) === false)
        assert(exists(initiallyMissingOutput1.pathInHost) === false)
        assert(exists(presentOutput.pathInHost))
        
        doInThreadAfter(200) {
          Files.writeTo(initiallyMissingOutput0.pathInHost)("0")
        }
        
        doInThreadAfter(100) {
          Files.writeTo(initiallyMissingOutput1.pathInHost)("1")
        }
        
        Await.result(f, 3.seconds)
        
        assert(exists(initiallyMissingOutput0.pathInHost))
        assert(exists(initiallyMissingOutput1.pathInHost))
        assert(exists(presentOutput.pathInHost))
      }
    }
    
    //Non-docker case: where the paths in the Outputs (from Loam files) are the same as those on the host FS 
    doTest(
        _ / "foo.txt",
        _ / "bar.txt",
        _ / "baz.txt",
        _ => Locations.identity)
        
    //Docker case: where the paths in the Outputs (from Loam files) are DIFFERNT from those on the host FS
    doTest( 
      _ => path("foo.txt"),
      _ => path("bar.txt"),
      _ => path("baz.txt"),
      outDir => MockLocations.fromFunctions(makeInHost = outDir.resolve(_)))
  }
  
  test("waitForOutputsOnly - no missing outputs") {
    import PathEnrichments.PathHelpers
    import ExecuterHelpers.waitForOutputsOnly
    import java.nio.file.Files.exists
    import TestHelpers.path
    
    def doTest(
        makeOut0: Path => Path,
        makeOut1: Path => Path,
        getLocations: Path => Locations[Path]): Unit = TestHelpers.withWorkDir(getClass.getSimpleName) { outDir =>
    
      val locations = getLocations(outDir)
            
      val out0 = makeOut0(outDir)
      val out1 = makeOut1(outDir)
      
      val output0 = Output.PathOutput(out0, locations)
      val output1 = Output.PathOutput(out1, locations)
      
      val mockJob = MockJob(
          toReturn = JobStatus.Succeeded,
          outputs = Set(output0, output1))
          
      //sanity check
      assert(exists(output0.pathInHost) === false)
      assert(exists(output1.pathInHost) === false)
          
      Files.writeTo(output0.pathInHost)("0")
      Files.writeTo(output1.pathInHost)("1")
      
      assert(exists(output0.pathInHost))
      assert(exists(output1.pathInHost))
          
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global
      
      withFileMonitor(new FileMonitor(10.0, 0.seconds)) { fileMonitor => 
        val f = waitForOutputsOnly(mockJob, fileMonitor)
        
        assert(f.isCompleted)
        
        assert(exists(output0.pathInHost))
        assert(exists(output1.pathInHost))
      }
    }
    
    //Non-docker case: where the paths in the Outputs (from Loam files) are the same as those on the host FS 
    doTest(
        _ / "foo.txt",
        _ / "bar.txt",
        _ => Locations.identity)
        
        
    //Docker case: where the paths in the Outputs (from Loam files) are DIFFERNT from those on the host FS
    doTest( 
      _ => path("foo.txt"),
      _ => path("bar.txt"),
      outDir => MockLocations.fromFunctions(makeInHost = outDir.resolve(_)))
  }
  
  test("waitForOutputsAndMakeExecution - success, no missing outputs") {
    
    import PathEnrichments.PathHelpers
    import ExecuterHelpers.waitForOutputsAndMakeExecution
    import java.nio.file.Files.exists
    import TestHelpers.path
    
    def doTest(
        makeOut0: Path => Path,
        makeOut1: Path => Path,
        getLocations: Path => Locations[Path]): Unit = TestHelpers.withWorkDir(getClass.getSimpleName) { outDir =>
    
      val locations = getLocations(outDir)
      val out0 = makeOut0(outDir)
      val out1 = makeOut1(outDir)
      
      val output0 = Output.PathOutput(out0, locations)
      val output1 = Output.PathOutput(out1, locations)
      
      val mockJob = MockJob(
          toReturn = JobStatus.Succeeded,
          outputs = Set(output0, output1))
          
      //sanity check
      assert(exists(output0.pathInHost) === false)
      assert(exists(output1.pathInHost) === false)  
          
      Files.writeTo(output0.pathInHost)("0")
      Files.writeTo(output1.pathInHost)("1")
      
      assert(exists(output0.pathInHost))
      assert(exists(output1.pathInHost))
          
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global
      
      val runData = RunData(mockJob, JobStatus.Succeeded, Some(JobResult.Success))
      
      withFileMonitor(new FileMonitor(10.0, 0.seconds)) { fileMonitor => 
        val f = waitForOutputsAndMakeExecution(runData, fileMonitor)
        
        val execution = Await.result(f, 10.seconds)
        
        assert(execution === runData.toExecution)
      }
    }
    
    //Non-docker case: where the paths in the Outputs (from Loam files) are the same as those on the host FS 
    doTest(
        _ / "foo.txt",
        _ / "bar.txt",
        _ => Locations.identity)
        
        
    //Docker case: where the paths in the Outputs (from Loam files) are DIFFERNT from those on the host FS
    doTest( 
      _ => path("foo.txt"),
      _ => path("bar.txt"),
      outDir => MockLocations.fromFunctions(makeInHost = outDir.resolve(_)))
  }
  
  test("waitForOutputsAndMakeExecution - success, some missing outputs") {
    import PathEnrichments.PathHelpers
    import ExecuterHelpers.waitForOutputsAndMakeExecution
    import java.nio.file.Files.exists
    import TestHelpers.path
    
    def doTest(
        makeOut0: Path => Path,
        makeOut1: Path => Path,
        getLocations: Path => Locations[Path]): Unit = TestHelpers.withWorkDir(getClass.getSimpleName) { outDir =>
    
      val locations = getLocations(outDir)
      val out0 = makeOut0(outDir)
      val out1 = makeOut1(outDir)
      
      val output0 = Output.PathOutput(out0, locations)
      val output1 = Output.PathOutput(out1, locations)
      
      val mockJob = MockJob(
          toReturn = JobStatus.Succeeded,
          outputs = Set(output0, output1))
        
      //sanity check
      assert(exists(output0.pathInHost) === false)
      assert(exists(output1.pathInHost) === false)
          
      Files.writeTo(output0.pathInHost)("0")
      
      assert(exists(output0.pathInHost))
      assert(exists(output1.pathInHost) === false)
          
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global
      
      val runData = RunData(mockJob, JobStatus.Succeeded, Some(JobResult.Success))
      
      withFileMonitor(new FileMonitor(10.0, 5.seconds)) { fileMonitor => 
        val f = waitForOutputsAndMakeExecution(runData, fileMonitor)
        
        assert(exists(output0.pathInHost))
        assert(exists(output1.pathInHost) === false)
        
        doInThread {
          Thread.sleep(100)
          
          Files.writeTo(output1.pathInHost)("1")
        }
        
        val execution = Await.result(f, 10.seconds)
        
        assert(exists(output0.pathInHost))
        assert(exists(output1.pathInHost))
        
        assert(execution === runData.toExecution)
      }
    }
    
    //Non-docker case: where the paths in the Outputs (from Loam files) are the same as those on the host FS 
    doTest(
        _ / "foo.txt",
        _ / "bar.txt",
        _ => Locations.identity)
        
        
    //Docker case: where the paths in the Outputs (from Loam files) are DIFFERNT from those on the host FS
    doTest( 
      _ => path("foo.txt"),
      _ => path("bar.txt"),
      outDir => MockLocations.fromFunctions(makeInHost = outDir.resolve(_)))
  }
  
  test("waitForOutputsAndMakeExecution - success, some missing outputs that don't appear in time") {
    import PathEnrichments.PathHelpers
    import ExecuterHelpers.waitForOutputsAndMakeExecution
    import java.nio.file.Files.exists
    import TestHelpers.path
    
    def doTest(
        makeOut0: Path => Path,
        makeOut1: Path => Path,
        getLocations: Path => Locations[Path]): Unit = TestHelpers.withWorkDir(getClass.getSimpleName) { outDir =>
    
      val locations = getLocations(outDir)
      val out0 = makeOut0(outDir)
      val out1 = makeOut1(outDir)
      
      val output0 = Output.PathOutput(out0, locations)
      val output1 = Output.PathOutput(out1, locations)
      
      val mockJob = MockJob(
          toReturn = JobStatus.Succeeded,
          outputs = Set(output0, output1))
        
      //sanity check
      assert(exists(output0.pathInHost) === false)
      assert(exists(output1.pathInHost) === false)
          
      Files.writeTo(output0.pathInHost)("0")
      
      assert(exists(output0.pathInHost))
      assert(exists(output1.pathInHost) === false)
          
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global
      
      val runData = RunData(mockJob, JobStatus.Succeeded, Some(JobResult.Success))
      
      //NB: Don't wait for any amount of time
      withFileMonitor(new FileMonitor(10.0, 0.seconds)) { fileMonitor => 
        val f = waitForOutputsAndMakeExecution(runData, fileMonitor)
        
        assert(exists(output0.pathInHost))
        assert(exists(output1.pathInHost) === false)
        
        //NB: Output will appear in 1 second, but we only "wait" for 0 seconds 
        doInThread {
          Thread.sleep(1000)
          
          Files.writeTo(output1.pathInHost)("1")
        }
        
        val execution = Await.result(f, 10.seconds)
        
        assert(execution.status === JobStatus.FailedWithException)
      }
    }
    
    //Non-docker case: where the paths in the Outputs (from Loam files) are the same as those on the host FS 
    doTest(
        _ / "foo.txt",
        _ / "bar.txt",
        _ => Locations.identity)
        
        
    //Docker case: where the paths in the Outputs (from Loam files) are DIFFERNT from those on the host FS
    doTest( 
      _ => path("foo.txt"),
      _ => path("bar.txt"),
      outDir => MockLocations.fromFunctions(makeInHost = outDir.resolve(_)))
  }
  
  test("waitForOutputsAndMakeExecution - failure") {
    import ExecuterHelpers.waitForOutputsAndMakeExecution
    
    val mockJob = MockJob(
        toReturn = JobStatus.FailedPermanently,
        outputs = Set.empty)
        
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val runData = RunData(mockJob, JobStatus.FailedPermanently, Some(JobResult.Failure))
    
    withFileMonitor(new FileMonitor(10.0, 5.seconds)) { fileMonitor => 
      val f = waitForOutputsAndMakeExecution(runData, fileMonitor)
      
      //NB: Outputs aren't considered at all, an already-completed future should be returned
      
      assert(f.isCompleted)
      
      val execution = Await.result(f, 10.seconds)
      
      assert(execution === runData.toExecution)
    }
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
  
  private def withFileMonitor[A](m: FileMonitor)(body: FileMonitor => A): A = try { body(m) } finally { m.stop() }
}
