package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.TestHelpers
import loamstream.model.jobs.DataHandle
import loamstream.util.Files

/**
 * @author clint
 * Nov 14, 2018
 */
final class JobFilterTest extends FunSuite {
  import JobFilterTest.MockJobFilter
  
  val alwaysSaysRun: JobFilter = new MockJobFilter(toReturn = true)
  val neverSaysRun: JobFilter = new MockJobFilter(toReturn = false)
  
  test("&&") {
    val t = alwaysSaysRun
    val f = neverSaysRun
    
    val job = MockJob(JobStatus.Succeeded)
    
    /*
     *  T && T => T
     *  T && F => F
     *  F && T => F
     *  F && F => F
     */
    assert((t && t).shouldRun(job) === true)
    assert((t && f).shouldRun(job) === false)
    assert((f && t).shouldRun(job) === false)
    assert((f && f).shouldRun(job) === false)
  }
  
  test("||") {
    val t = alwaysSaysRun
    val f = neverSaysRun
    
    val job = MockJob(JobStatus.Succeeded)
    
    /*
     *  T && T => T
     *  T && F => T
     *  F && T => T
     *  F && F => F
     */
    assert((t || t).shouldRun(job) === true)
    assert((t || f).shouldRun(job) === true)
    assert((f || t).shouldRun(job) === true)
    assert((f || f).shouldRun(job) === false)
  }
  
  test("WithAnyMissingOutputs") {
    import JobFilter.WithAnyMissingOutputs.shouldRun
    import java.nio.file.Files.exists
    
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val pathA = workDir.resolve("A")
      val pathB = workDir.resolve("B")
      val missingPath = workDir.resolve("C")
      
      Files.writeTo(pathA)("AAA")
      Files.writeTo(pathB)("BBB")
      
      val outputA = DataHandle.PathHandle(pathA)
      val outputB = DataHandle.PathHandle(pathB)
      val outputMissing = DataHandle.PathHandle(missingPath)
      
      assert(outputA.isPresent === true)
      assert(outputB.isPresent === true)
      assert(outputMissing.isPresent === false)
      
      def jobWithOutputs(outputs: DataHandle*): LJob = MockJob(JobStatus.Succeeded, outputs = outputs.toSet)      
      
      val noOutputs = jobWithOutputs()
      val noMissingOutputs = jobWithOutputs(outputA, outputB)
      val someMissingOutputs = jobWithOutputs(outputA, outputMissing, outputB)
      
      assert(shouldRun(noOutputs) === true)
      assert(shouldRun(noMissingOutputs) === false)
      assert(shouldRun(someMissingOutputs) === true)
    }
  }
}

object JobFilterTest {
  private final class MockJobFilter(toReturn: Boolean) extends JobFilter {
    override def shouldRun(job: LJob): Boolean = toReturn
  }
}
