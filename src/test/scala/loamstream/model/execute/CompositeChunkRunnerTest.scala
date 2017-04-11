package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.jobs.{Execution, JobStatus, LJob, MockJob}

import scala.concurrent.ExecutionContext
import loamstream.util.Futures
import rx.lang.scala.Observable
import loamstream.util.ObservableEnrichments
import loamstream.TestHelpers

/**
 * @author clint
 * Nov 23, 2016
 */
final class CompositeChunkRunnerTest extends FunSuite {
  
  //scalastyle:off magic.number
  
  private def local(n: Int) = AsyncLocalChunkRunner(n)(ExecutionContext.global)
  
  import TestHelpers.neverRestart
  
  test("maxNumJobs") {
    val n1 = 3
    val n2 = 5
    val n3 = 1
    
    val runner = CompositeChunkRunner(Seq(local(n1), local(n2), local(n3)))
    
    assert(runner.maxNumJobs === (n1 + n2 + n3))
  }

  private final case class MockRunner(allowed: MockJob) extends ChunkRunner {
    override def maxNumJobs: Int = ???
    
    override def canRun(job: LJob): Boolean = job eq allowed
    
    private val delegate = local(1)
    
    override def run(jobs: Set[LJob], shouldRestart: LJob => Boolean): Observable[Map[LJob, Execution]] = {
      delegate.run(jobs, shouldRestart)
    }
  }
  
  test("canRun") {

    val job1 = MockJob(JobStatus.Succeeded)
    val job2 = MockJob(JobStatus.Succeeded)
    val job3 = MockJob(JobStatus.Succeeded)
    val job4 = MockJob(JobStatus.Succeeded)
    
    {
      val runner = CompositeChunkRunner(Seq(MockRunner(job1), MockRunner(job2), MockRunner(job3)))
      
      assert(runner.canRun(job1))
      assert(runner.canRun(job2))
      assert(runner.canRun(job3))
      assert(runner.canRun(job4) === false)
    }
  }
  
  test("run") {
    val job1 = MockJob(JobStatus.Succeeded)
    val job2 = MockJob(JobStatus.Failed)
    val job3 = MockJob(JobStatus.Succeeded)
    val job4 = MockJob(JobStatus.Succeeded)
    
    val runner = CompositeChunkRunner(Seq(MockRunner(job1), MockRunner(job2)))
    
    import scala.concurrent.ExecutionContext.Implicits.global
    
    //Should throw if we can't run all the given jobs
    intercept[Exception] {
      runner.run(Set(job2, job3), neverRestart)
    }
    
    intercept[Exception] {
      runner.run(Set(job1, job4), neverRestart)
    }
    
    import ObservableEnrichments._
    
    val futureResults = runner.run(Set(job1, job2), neverRestart).firstAsFuture
    
    val expected = Map(job1 -> JobStatus.Succeeded, job2 -> JobStatus.Failed)
    
    assert(Futures.waitFor(futureResults).mapValues(_.status) === expected)
  }
  
  //scalastyle:on magic.number
}
