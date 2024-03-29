package loamstream.model.execute

import java.nio.file.Path

import scala.concurrent.Await
import scala.concurrent.Future

import loamstream.LoamFunSuite
import loamstream.TestHelpers
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.RunData
import loamstream.model.jobs.RxMockJob
import loamstream.model.jobs.TestJobs
import loamstream.util.FileMonitor
import loamstream.util.Files
import loamstream.util.Paths
import monix.eval.Task


/**
 * @author clint
 * date: Jun 7, 2016
 */
final class ExecuterHelpersTest extends LoamFunSuite with TestJobs {
  
  test("statusAndResultFrom") {
    import ExecuterHelpers.statusAndResultFrom
    
    val e = new Exception with scala.util.control.NoStackTrace
    
    val (status, result) = statusAndResultFrom(e)
    
    assert(status === JobStatus.FailedWithException)
    
    assert(result === JobResult.CommandInvocationFailure(e))
  }
  
  test("updateWithException") {
    val execution = Execution(
        settings = LocalSettings,
        status = JobStatus.Running,
        result = None,
        jobDir = None,
        terminationReason = None)
        
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
    
    import Paths.Implicits.PathHelpers
    import ExecuterHelpers.waitForOutputsOnly
    import java.nio.file.Files.exists
    import TestHelpers.path
    
    def doTest(
        makeInitiallyMissing0: Path => Path,
        makeInitiallyMissing1: Path => Path,
        makePresent: Path => Path): Unit = TestHelpers.withWorkDir(getClass.getSimpleName) { outDir =>
          
      val initiallyMissing0 = makeInitiallyMissing0(outDir)
      val initiallyMissing1 = makeInitiallyMissing1(outDir)
      val present = makePresent(outDir)
      
      val initiallyMissingOutput0 = DataHandle.PathHandle(initiallyMissing0)
      val initiallyMissingOutput1 = DataHandle.PathHandle(initiallyMissing1)
      val presentOutput = DataHandle.PathHandle(present)
      
      val mockJob = MockJob(
          toReturn = JobStatus.Succeeded,
          outputs = Set(initiallyMissingOutput0, initiallyMissingOutput1, presentOutput))
          
      Files.writeTo(presentOutput.path)("2")
      
      assert(exists(initiallyMissingOutput0.path) === false)
      assert(exists(initiallyMissingOutput1.path) === false)
      assert(exists(presentOutput.path))
      
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global
      
      withFileMonitor(new FileMonitor(10.0, 2.seconds)) { fileMonitor =>
        val f = waitForOutputsOnly(mockJob, fileMonitor)
        
        assert(exists(initiallyMissingOutput0.path) === false)
        assert(exists(initiallyMissingOutput1.path) === false)
        assert(exists(presentOutput.path))
        
        doInThreadAfter(200) {
          Files.writeTo(initiallyMissingOutput0.path)("0")
        }
        
        doInThreadAfter(100) {
          Files.writeTo(initiallyMissingOutput1.path)("1")
        }
        
        TestHelpers.waitForT(f, 3.seconds)
        
        assert(exists(initiallyMissingOutput0.path))
        assert(exists(initiallyMissingOutput1.path))
        assert(exists(presentOutput.path))
      }
    }
    
    doTest(
        _ / "foo.txt",
        _ / "bar.txt",
        _ / "baz.txt")
  }
  
  test("waitForOutputsOnly - no missing outputs") {
    import Paths.Implicits.PathHelpers
    import ExecuterHelpers.waitForOutputsOnly
    import java.nio.file.Files.exists
    import TestHelpers.path
    
    def doTest(
        makeOut0: Path => Path,
        makeOut1: Path => Path): Unit = TestHelpers.withWorkDir(getClass.getSimpleName) { outDir =>
    
      val out0 = makeOut0(outDir)
      val out1 = makeOut1(outDir)
      
      val output0 = DataHandle.PathHandle(out0)
      val output1 = DataHandle.PathHandle(out1)
      
      val mockJob = MockJob(
          toReturn = JobStatus.Succeeded,
          outputs = Set(output0, output1))
          
      //sanity check
      assert(exists(output0.path) === false)
      assert(exists(output1.path) === false)
          
      Files.writeTo(output0.path)("0")
      Files.writeTo(output1.path)("1")
      
      assert(exists(output0.path))
      assert(exists(output1.path))
          
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global
      
      withFileMonitor(new FileMonitor(10.0, 0.seconds)) { fileMonitor => 
        val f = waitForOutputsOnly(mockJob, fileMonitor)
        
        TestHelpers.waitForT(f)
        
        assert(exists(output0.path))
        assert(exists(output1.path))
      }
    }
    
    doTest(
        _ / "foo.txt",
        _ / "bar.txt")
  }
  
  test("waitForOutputsAndMakeExecution - success, no missing outputs") {
    import Paths.Implicits.PathHelpers
    import ExecuterHelpers.waitForOutputsAndMakeExecution
    import java.nio.file.Files.exists
    import TestHelpers.path
    
    def doTest(
        makeOut0: Path => Path,
        makeOut1: Path => Path): Unit = TestHelpers.withWorkDir(getClass.getSimpleName) { outDir =>
    
      val out0 = makeOut0(outDir)
      val out1 = makeOut1(outDir)
      
      val output0 = DataHandle.PathHandle(out0)
      val output1 = DataHandle.PathHandle(out1)
      
      val mockJob = MockJob(
          toReturn = JobStatus.Succeeded,
          outputs = Set(output0, output1))
          
      //sanity check
      assert(exists(output0.path) === false)
      assert(exists(output1.path) === false)  
          
      Files.writeTo(output0.path)("0")
      Files.writeTo(output1.path)("1")
      
      assert(exists(output0.path))
      assert(exists(output1.path))
          
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global
      
      val runData = RunData(
          mockJob, 
          LocalSettings, 
          JobStatus.Succeeded, 
          Some(JobResult.Success), 
          terminationReasonOpt = None)
      
      withFileMonitor(new FileMonitor(10.0, 0.seconds)) { fileMonitor => 
        val f = waitForOutputsAndMakeExecution(runData, fileMonitor)
        
        val execution = TestHelpers.waitForT(f, 10.seconds)
        
        assert(execution === runData.toExecution)
      }
    }
    
    doTest(
        _ / "foo.txt",
        _ / "bar.txt")
  }
  
  test("waitForOutputsAndMakeExecution - success, some missing outputs") {
    import Paths.Implicits.PathHelpers
    import ExecuterHelpers.waitForOutputsAndMakeExecution
    import java.nio.file.Files.exists
    import TestHelpers.path
    
    def doTest(
        makeOut0: Path => Path,
        makeOut1: Path => Path): Unit = TestHelpers.withWorkDir(getClass.getSimpleName) { outDir =>
    
      val output0 = DataHandle.PathHandle(makeOut0(outDir))
      val output1 = DataHandle.PathHandle(makeOut1(outDir))
      
      val mockJob = MockJob(
          toReturn = JobStatus.Succeeded,
          outputs = Set(output0, output1))
        
      //sanity check
      assert(exists(output0.path) === false)
      assert(exists(output1.path) === false)
          
      Files.writeTo(output0.path)("0")
      
      assert(exists(output0.path))
      assert(exists(output1.path) === false)
          
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global

      val runData = RunData(
          mockJob, 
          LocalSettings, 
          JobStatus.Succeeded, 
          Some(JobResult.Success), 
          terminationReasonOpt = None)
      
      withFileMonitor(new FileMonitor(10.0, 5.seconds)) { fileMonitor => 
        val f = waitForOutputsAndMakeExecution(runData, fileMonitor)
        
        assert(exists(output0.path))
        assert(exists(output1.path) === false)
        
        doInThread {
          Thread.sleep(100)
          
          Files.writeTo(output1.path)("1")
        }
        
        val execution = TestHelpers.waitForT(f, 10.seconds)
        
        assert(exists(output0.path))
        assert(exists(output1.path))
        
        assert(execution === runData.toExecution)
      }
    }
    
    doTest(
        _ / "foo.txt",
        _ / "bar.txt")
  }
  
  test("waitForOutputsAndMakeExecution - success, some missing outputs that don't appear in time") {
    import Paths.Implicits.PathHelpers
    import ExecuterHelpers.waitForOutputsAndMakeExecution
    import java.nio.file.Files.exists
    import TestHelpers.path
    
    def doTest(
        makeOut0: Path => Path,
        makeOut1: Path => Path): Unit = TestHelpers.withWorkDir(getClass.getSimpleName) { outDir =>
    
      val output0 = DataHandle.PathHandle(makeOut0(outDir))
      val output1 = DataHandle.PathHandle(makeOut1(outDir))
      
      val mockJob = MockJob(
          toReturn = JobStatus.Succeeded,
          outputs = Set(output0, output1))
        
      //sanity check
      assert(exists(output0.path) === false)
      assert(exists(output1.path) === false)
          
      Files.writeTo(output0.path)("0")

      assert(exists(output0.path))
      assert(exists(output1.path) === false)
          
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global
      
      val runData = RunData(
          mockJob, 
          LocalSettings,
          JobStatus.Succeeded, 
          Some(JobResult.Success), 
          terminationReasonOpt = None)
      
      //NB: Don't wait for any amount of time
      withFileMonitor(new FileMonitor(10.0, 0.seconds)) { fileMonitor => 
        val f = waitForOutputsAndMakeExecution(runData, fileMonitor)
        
        assert(exists(output0.path))
        assert(exists(output1.path) === false)
        
        //NB: Output will appear in 1 second, but we only "wait" for 0 seconds 
        doInThread {
          Thread.sleep(1000)
          
          Files.writeTo(output1.path)("1")
        }
        
        val execution = TestHelpers.waitForT(f, 10.seconds)
        
        assert(execution.status === JobStatus.FailedWithException)
      }
    }
    
    doTest(
        _ / "foo.txt",
        _ / "bar.txt")
  }
  
  test("waitForOutputsAndMakeExecution - failure") {
    import ExecuterHelpers.waitForOutputsAndMakeExecution
    
    val mockJob = MockJob(
        toReturn = JobStatus.FailedPermanently,
        outputs = Set.empty)
        
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val runData = RunData(
        mockJob,
        LocalSettings,
        JobStatus.FailedPermanently, 
        Some(JobResult.Failure), 
        terminationReasonOpt = None)
    
    withFileMonitor(new FileMonitor(10.0, 5.seconds)) { fileMonitor => 
      val f = waitForOutputsAndMakeExecution(runData, fileMonitor)
      
      //NB: Outputs aren't considered at all, an already-completed future should be returned
      
      val execution = TestHelpers.waitForT(f, 10.seconds)
      
      assert(execution === runData.toExecution)
    }
  }
  
  test("doWaiting - no exception") {
    import ExecuterHelpers.doWaiting
    
    val mockJob = MockJob(JobStatus.Succeeded)
    
    val alreadyComplete = Task.now(())
    
    def expected = mockJob.toReturn.toExecution
    
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val f = doWaiting(alreadyComplete, mockJob.toReturn.toExecution, throw new Exception)
    
    val execution = TestHelpers.waitForT(f, 5.seconds)
    
    assert(execution === expected)
  }
  
  test("doWaiting - exception thrown while making Execution") {
    import ExecuterHelpers.doWaiting
    
    val mockJob = MockJob(JobStatus.Succeeded)
    
    val alreadyComplete = Task.now(())
    
    def fallbackExecution = Execution.from(mockJob, JobStatus.FailedPermanently, terminationReason = None)
    
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val f = doWaiting(alreadyComplete, throw new Exception("blerg"), fallbackExecution)
    
    val execution = TestHelpers.waitForT(f, 5.seconds)
    
    assert(execution === fallbackExecution)
  }
  
  test("doWaiting - exception thrown while waiting") {
    import ExecuterHelpers.doWaiting
    
    val mockJob = MockJob(JobStatus.Succeeded)
    
    val exception = new Exception with scala.util.control.NoStackTrace
    
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val waitFuture = Task.raiseError(exception)
    
    def fallbackExecution = Execution.from(mockJob, JobStatus.FailedPermanently, terminationReason = None)
    
    import scala.concurrent.duration._
    
    val f = doWaiting(waitFuture, mockJob.toReturn.toExecution, fallbackExecution)
    
    val execution = TestHelpers.waitForT(f, 5.seconds)
    
    assert(execution.status === JobStatus.FailedWithException)
    assert(execution.result === Some(JobResult.CommandInvocationFailure(exception)))
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
  
  private def withFileMonitor[A](m: FileMonitor)(body: FileMonitor => A): A = try { body(m) } finally { m.stop() }
}
