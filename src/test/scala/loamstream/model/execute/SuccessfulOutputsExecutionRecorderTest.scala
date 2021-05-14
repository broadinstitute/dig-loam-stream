package loamstream.model.execute

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.util.Files
import scala.collection.compat._

/**
 * @author clint
 * Dec 18, 2020
 */
final class SuccessfulOutputsExecutionRecorderTest extends FunSuite {
  test("SuccessfulOutputsExecutionRecorder") {
    import TestHelpers.path
    
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("successful-outputs.txt")
      
      val recorder = SuccessfulOutputsExecutionRecorder(file)
      
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
      
      val recordedLocs = Files.readFrom(file).split(System.lineSeparator).iterator.map(_.trim).to(Set)
      
      val expected = for {
        job <- Seq(j0, j2, j3, j4)
        output <- job.outputs.to(Seq)
      } yield {
        s"${job.toReturn.jobStatus}\t${output.location}"
      }
      
      assert(recordedLocs === expected.to(Set))
    }
  }
}
