package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.LJob
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
    
    override def run(jobs: Set[LJob]): Observable[Map[LJob, JobResult]] = delegate.run(jobs)
  }
  
  test("canRun") {
    import JobResult.Succeeded
    
    val job1 = MockJob(Succeeded)
    val job2 = MockJob(Succeeded)
    val job3 = MockJob(Succeeded)
    val job4 = MockJob(Succeeded)
    
    {
      val runner = CompositeChunkRunner(Seq(MockRunner(job1), MockRunner(job2), MockRunner(job3)))
      
      assert(runner.canRun(job1))
      assert(runner.canRun(job2))
      assert(runner.canRun(job3))
      assert(runner.canRun(job4) === false)
    }
  }
  
  test("run") {
    import JobResult.{Succeeded,Failed}
    
    val job1 = MockJob(Succeeded)
    val job2 = MockJob(Failed(Some(TestHelpers.localResources)))
    val job3 = MockJob(Succeeded)
    val job4 = MockJob(Succeeded)
    
    val runner = CompositeChunkRunner(Seq(MockRunner(job1), MockRunner(job2)))
    
    import scala.concurrent.ExecutionContext.Implicits.global
    
    //Should throw if we can't run all the given jobs
    intercept[Exception] {
      runner.run(Set(job2, job3))
    }
    
    intercept[Exception] {
      runner.run(Set(job1, job4))
    }
    
    import ObservableEnrichments._
    
    val futureResults = runner.run(Set(job1, job2)).firstAsFuture
    
    val expected = Map(job1 -> Succeeded, job2 -> Failed(Some(TestHelpers.localResources))) 
    
    assert(Futures.waitFor(futureResults) === expected)
  }
  
  //scalastyle:on magic.number
}
