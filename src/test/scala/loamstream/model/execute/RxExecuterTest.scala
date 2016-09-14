package loamstream.model.execute

import loamstream.conf.UgerConfig
import loamstream.model.execute.RxExecuter.asyncLocalChunkRunner
import loamstream.model.execute.RxExecuter.RxMockJob
import loamstream.model.execute.RxExecuterTest.MockChunkRunner
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import org.scalatest.FunSuite

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import rx.lang.scala.Observable

/**
 * @author kyuksel
 *         date: Aug 17, 2016
 */
final class RxExecuterTest extends FunSuite {
  
  test("A single-job pipeline works") {
    /* A one-step pipeline:
     *
     * Job1
     * 
     */

    val executer = RxExecuter.default
    
    val job1 = RxMockJob("Job_1")

    assert(job1.executionCount === 0)

    val executable = LExecutable(Set(job1))
    val result = executer.execute(executable)

    assert(job1.executionCount === 1)

    assert(result.values.head.get.isSuccess)
    assert(result.size === 1)
    
    val jobExecutionSeq = executer.tracker.jobExecutionSeq
    
    assert(jobExecutionSeq == Seq(Set(job1)))
  }
  
  // scalastyle:off magic.number
  test("2-job linear pipeline works") {
    /* A 2-step pipeline:
     *
     * Job1 -- Job2
     * 
     */

    val executer = RxExecuter.default
    
    val job1 = RxMockJob("Job_1")
    val job2 = RxMockJob("Job_2", Set(job1))

    assert(job1.executionCount === 0)
    assert(job2.executionCount === 0)

    val executable = LExecutable(Set(job2))
    
    val result = executer.execute(executable)

    assert(job1.executionCount === 1)
    assert(job2.executionCount === 1)

    assert(result.values.toSeq.apply(0).get.isSuccess)
    assert(result.values.toSeq.apply(1).get.isSuccess)
    assert(result.size === 2)
    
    val jobExecutionSeq = executer.tracker.jobExecutionSeq
    
    assert(jobExecutionSeq == Seq(Set(job1), Set(job2)))
  }
  
  test("3-job linear pipeline works") {
    /* A 3-step pipeline:
     *
     * Job1 -- Job2 -- Job3
     * 
     */

    val executer = RxExecuter.default
    
    val job1 = RxMockJob("Job_1")
    val job2 = RxMockJob("Job_2", Set(job1))
    val job3 = RxMockJob("Job_3", Set(job2))

    assert(job1.executionCount === 0)
    assert(job2.executionCount === 0)
    assert(job3.executionCount === 0)

    val executable = LExecutable(Set(job3))
    
    val result = executer.execute(executable)

    assert(job1.executionCount === 1)
    assert(job2.executionCount === 1)
    assert(job3.executionCount === 1)

    assert(result.values.toSeq.apply(0).get.isSuccess)
    assert(result.values.toSeq.apply(1).get.isSuccess)
    assert(result.values.toSeq.apply(2).get.isSuccess)
    
    assert(result.size === 3)
    
    val jobExecutionSeq = executer.tracker.jobExecutionSeq
    
    assert(jobExecutionSeq == Seq(Set(job1), Set(job2), Set(job3)))
  }
  
  test("3-job pipeline with multiple dependencies works") {
    /* A 3-step pipeline:
     *
     * Job1 
     *     \
     *       -- Job3
     *     /
     * Job2
     */

    val executer = RxExecuter.default
    
    val job1 = RxMockJob("Job_1")
    val job2 = RxMockJob("Job_2")
    val job3 = RxMockJob("Job_3", Set(job1, job2))

    assert(job1.executionCount === 0)
    assert(job2.executionCount === 0)
    assert(job3.executionCount === 0)

    val executable = LExecutable(Set(job3))
    
    val result = executer.execute(executable)

    assert(job1.executionCount === 1)
    assert(job2.executionCount === 1)
    assert(job3.executionCount === 1)

    assert(result.size === 3)
    
    assert(result.values.toSeq.apply(0).get.isSuccess)
    assert(result.values.toSeq.apply(1).get.isSuccess)
    assert(result.values.toSeq.apply(2).get.isSuccess)
  }
  
  test("3-job pipeline with two 'roots'") {
    /* A 3-step pipeline:
     *
     *          Job2 
     *         /
     * Job1 --
     *         \
     *          Job3
     */

    val executer = RxExecuter.default
    
    val job1 = RxMockJob("Job_1")
    val job2 = RxMockJob("Job_2", Set(job1))
    val job3 = RxMockJob("Job_3", Set(job1))

    assert(job1.executionCount === 0)
    assert(job2.executionCount === 0)
    assert(job3.executionCount === 0)

    val executable = LExecutable(Set(job2, job3))
    
    val result = executer.execute(executable)

    assert(job1.executionCount === 1)
    assert(job2.executionCount === 1)
    assert(job3.executionCount === 1)

    assert(result.size === 3)
    
    assert(result.values.toSeq.apply(0).get.isSuccess)
    assert(result.values.toSeq.apply(1).get.isSuccess)
    assert(result.values.toSeq.apply(2).get.isSuccess)
  }
  
  test("Diamond-shaped pipeline works") {
    /* A 3-step pipeline:
     *
     *        Job2 
     *       /    \
     * Job1--      -- Job4
     *       \    /
     *        Job3
     */

    val executer = RxExecuter.default
    
    val job1 = RxMockJob("Job_1")
    val job2 = RxMockJob("Job_2", Set(job1))
    val job3 = RxMockJob("Job_3", Set(job1))
    val job4 = RxMockJob("Job_4", Set(job2, job3))

    assert(job1.executionCount === 0)
    assert(job2.executionCount === 0)
    assert(job3.executionCount === 0)
    assert(job4.executionCount === 0)

    val executable = LExecutable(Set(job4))
    
    val result = executer.execute(executable)

    assert(Seq(job1, job2, job3, job4).map(_.executionCount) == Seq(1,1,1,1))

    assert(result.size === 4)
    
    assert(result.values.toSeq.apply(0).get.isSuccess)
    assert(result.values.toSeq.apply(1).get.isSuccess)
    assert(result.values.toSeq.apply(2).get.isSuccess)
    assert(result.values.toSeq.apply(3).get.isSuccess)
  }
  
  // scalastyle:off magic.number
  test("New leaves are executed as soon as possible when there is no delay") {
    /* A four-step pipeline:
     *
     *           Job21
     *          /      \
     * Job11 --          -- Job31
     *          \      /         \
     *           Job22            \
     *                              -- Job4
     *           Job23            /
     *          /      \         /
     * Job12 --          -- Job32
     *          \      /
     *           Job24
     */

    val executer = RxExecuter.default

    val job11 = RxMockJob("Job_1_1")
    val job12 = RxMockJob("Job_1_2")
    val job21 = RxMockJob("Job_2_1", Set(job11))
    val job22 = RxMockJob("Job_2_2", Set(job11))
    val job23 = RxMockJob("Job_2_3", Set(job12))
    val job24 = RxMockJob("Job_2_4", Set(job12))
    val job31 = RxMockJob("Job_3_1", Set(job21, job22))
    val job32 = RxMockJob("Job_3_2", Set(job23, job24))
    val job4 = RxMockJob("Job_4", Set(job31, job32))

    assert(job11.executionCount === 0)
    assert(job12.executionCount === 0)
    assert(job21.executionCount === 0)
    assert(job22.executionCount === 0)
    assert(job23.executionCount === 0)
    assert(job24.executionCount === 0)
    assert(job31.executionCount === 0)
    assert(job32.executionCount === 0)
    assert(job4.executionCount === 0)

    val executable = LExecutable(Set(job4))
    val result = executer.execute(executable)

    assert(job11.executionCount === 1)
    assert(job12.executionCount === 1)
    assert(job21.executionCount === 1)
    assert(job22.executionCount === 1)
    assert(job23.executionCount === 1)
    assert(job24.executionCount === 1)
    assert(job31.executionCount === 1)
    assert(job32.executionCount === 1)
    assert(job4.executionCount === 1)

    assert(result.size === 9)

    // Check if jobs were correctly chunked
    val jobExecutionSeq = executer.tracker.jobExecutionSeq
    assert(jobExecutionSeq(0) === Set(job11, job12))
    assert(jobExecutionSeq(1) === Set(job21, job22, job23, job24))
    assert(jobExecutionSeq(2) === Set(job31, job32))
    assert(jobExecutionSeq(3) === Set(job4))
    assert(jobExecutionSeq.length === 4)
  }

  test("New leaves are executed as soon as possible when initial jobs don't start simultaneously") {
    //Vector(
      //Set(Job_1_1), 
      //Set(Job_2_1, Job_2_2), 
      //Set(Job_3_1, Job_1_2), 
      //Set(Job_2_3, Job_2_4), 
      //Set(Job_3_2), 
      //Set(Job_4))
    
    /* A four-step pipeline:
     *
     *           Job21
     *          /      \
     * Job11 --          -- Job31
     *          \      /         \
     *           Job22            \
     *                              -- Job4
     *           Job23            /
     *          /      \         /
     * Job12 --          -- Job32
     *          \      /
     *           Job24
     */

    val executer = RxExecuter.default

    // The delay added to job11 should cause job23 and job24 to be bundled and executed prior to job21 and job22
    lazy val job11 = RxMockJob("Job_1_1", delay = 1000)
    lazy val job12 = RxMockJob("Job_1_2")
    lazy val job21 = RxMockJob("Job_2_1", Set(job11))
    lazy val job22 = RxMockJob("Job_2_2", Set(job11))
    lazy val job23 = RxMockJob("Job_2_3", Set(job12), delay = 1000)
    lazy val job24 = RxMockJob("Job_2_4", Set(job12))
    lazy val job31 = RxMockJob("Job_3_1", Set(job21, job22))
    lazy val job32 = RxMockJob("Job_3_2", Set(job23, job24))
    lazy val job4 = RxMockJob("Job_4", Set(job31, job32))

    assert(job11.executionCount === 0)
    assert(job12.executionCount === 0)
    assert(job21.executionCount === 0)
    assert(job22.executionCount === 0)
    assert(job23.executionCount === 0)
    assert(job24.executionCount === 0)
    assert(job31.executionCount === 0)
    assert(job32.executionCount === 0)
    assert(job4.executionCount === 0)

    val executable = LExecutable(Set(job4))
    val result = executer.execute(executable)

    assert(job11.executionCount === 1)
    assert(job12.executionCount === 1)
    assert(job21.executionCount === 1)
    assert(job22.executionCount === 1)
    assert(job23.executionCount === 1)
    assert(job24.executionCount === 1)
    assert(job31.executionCount === 1)
    assert(job32.executionCount === 1)
    assert(job4.executionCount === 1)

    assert(result.size === 9)

    // Check if jobs were correctly chunked
    val jobExecutionSeq = executer.tracker.jobExecutionSeq
    
    assert(jobExecutionSeq(0) === Set(job12))
    assert(jobExecutionSeq(1) === Set(job11, job24))
    assert(jobExecutionSeq(2) === Set(job21, job22))
    assert(jobExecutionSeq(3) === Set(job31))
    assert(jobExecutionSeq(4) === Set(job23))
    assert(jobExecutionSeq(5) === Set(job32))
    assert(jobExecutionSeq(6) === Set(job4))
    
    assert(jobExecutionSeq.length === 7)
  }

  test("maxNumJobs is taken into account") {
    /* A four-step pipeline:
     *
     *           Job21
     *          /      \
     * Job11 --          -- Job31
     *          \      /         \
     *           Job22            \
     *                              -- Job4
     *           Job23            /
     *          /      \         /
     * Job12 --  Job24 - -- Job32
     *          \      /
     *           Job25
     */

    val job11 = RxMockJob("Job_1_1")
    val job12 = RxMockJob("Job_1_2")
    val job21 = RxMockJob("Job_2_1", Set(job11))
    val job22 = RxMockJob("Job_2_2", Set(job11))
    val job23 = RxMockJob("Job_2_3", Set(job12))
    val job24 = RxMockJob("Job_2_4", Set(job12))
    val job25 = RxMockJob("Job_2_5", Set(job12))
    val job31 = RxMockJob("Job_3_1", Set(job21, job22))
    val job32 = RxMockJob("Job_3_2", Set(job23, job24, job25))
    val job4 = RxMockJob("Job_4", Set(job31, job32))

    val executable = LExecutable(Set(job4))

    val maxSimultaneousJobs = 4
    val mockRunner = MockChunkRunner(asyncLocalChunkRunner(maxSimultaneousJobs))
    val executer = RxExecuter(mockRunner)

    executer.execute(executable)

    val chunks = mockRunner.chunks
    val expectedMaxSimultaneousJobs = 4
    val expectedNumberOfChunks = 5
    assert(chunks.size === expectedNumberOfChunks)
    assert(chunks.forall(_.size <= expectedMaxSimultaneousJobs))
  }
  // scalastyle:on magic.number
}

object RxExecuterTest {
  private final case class MockChunkRunner(delegate: ChunkRunner) extends ChunkRunner {
    override def maxNumJobs: Int = delegate.maxNumJobs
    
    var chunks: Seq[Set[LJob]] = Nil

    override def run(jobs: Set[LJob])(implicit context: ExecutionContext): Observable[Map[LJob, Result]] = {
      chunks = chunks :+ jobs

      delegate.run(jobs)
    }
  }
}
