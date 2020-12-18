package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.LJob
import loamstream.model.jobs.Execution
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.DataHandle
import loamstream.util.Files

/**
 * @author clint
 * May 28, 2019
 */
final class ExecutionRecorderTest extends FunSuite {
  test("&&") {
    var flag0 = 0
    var flag1 = 10
    
    def mockExecutionRecorder[A](body: => A): ExecutionRecorder = new ExecutionRecorder {
      override def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit = {
        body
        ()
      }
    }
    
    val er0 = mockExecutionRecorder { flag0 += 1}
    val er1 = mockExecutionRecorder { flag1 += 1}
    
    val composite = er0 && er1
    
    assert(flag0 === 0)
    assert(flag1 === 10)
    
    composite.record(TestHelpers.DummyJobOracle, Nil)
    
    assert(flag0 === 1)
    assert(flag1 === 11)
    
    composite.record(TestHelpers.DummyJobOracle, Nil)
    
    assert(flag0 === 2)
    assert(flag1 === 12)
  }
  
  test("SuccessfulOutputsExecutionRecorder") {
    import TestHelpers.path
    
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("successful-outputs.txt")
      
      val recorder = ExecutionRecorder.SuccessfulOutputsExecutionRecorder(file)
      
      val o0 = DataHandle.PathHandle(path("foo"))
      val o1 = DataHandle.PathHandle(path("bar"))
      val o2 = DataHandle.PathHandle(path("baz"))
      val o3 = DataHandle.PathHandle(path("nuh"))
      val o4 = DataHandle.PathHandle(path("zuh"))
      
      val j0 = MockJob(JobStatus.Succeeded, outputs = Set(o0))
      val j1 = MockJob(JobStatus.Failed, outputs = Set(o1))
      val j2 = MockJob(JobStatus.Succeeded, outputs = Set.empty)
      val j3 = MockJob(JobStatus.Skipped, outputs = Set(o2))
      val j4 = MockJob(JobStatus.Succeeded, outputs = Set(o3, o4))
      
      def toExecution(j: MockJob): Execution = Execution.from(j, j.toReturn.jobStatus)
      
      val e0 = toExecution(j0)
      val e1 = toExecution(j1)
      val e2 = toExecution(j2)
      val e3 = toExecution(j3)
      val e4 = toExecution(j4)
      
      val tuples = Seq(j0 -> e0, j1 -> e1, j2 -> e2, j3 -> e3, j4 -> e4)
      
      import java.nio.file.Files.exists
      
      assert(exists(file) === false)
      
      recorder.record(TestHelpers.DummyJobOracle, tuples)
      
      assert(exists(file) === true)
      
      val recordedLocs = Files.readFrom(file).split(System.lineSeparator).iterator.map(_.trim).toSet
      
      val expected = Seq(o0, o2, o3, o4).map(_.location).toSet
      
      assert(recordedLocs === expected)
    }
  }
}
