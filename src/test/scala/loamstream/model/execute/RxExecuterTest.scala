package loamstream.model.execute

import scala.concurrent.duration.DurationDouble

import org.scalatest.FunSuite

import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob
import loamstream.util.ValueBox
import rx.lang.scala.Observable
import loamstream.model.jobs.RxMockJob
import loamstream.oracle.Resources.LocalResources

/**
 * @author kyuksel
 *         date: Aug 17, 2016
 */
final class RxExecuterTest extends FunSuite {
  import RxExecuterTest.ExecutionResults
  import scala.concurrent.ExecutionContext.Implicits.global
  
  // scalastyle:off magic.number  
  private def exec(
      executable: Executable, 
      maxSimultaneousJobs: Int = 8): ExecutionResults = {
    
    import scala.concurrent.duration._
    
    val runner = MockChunkRunner(AsyncLocalChunkRunner(maxSimultaneousJobs))
    
    val executer = RxExecuter(runner, 0.25.seconds, JobFilter.RunEverything)
    
    ExecutionResults(executer.execute(executable), runner.chunks.value.filter(_.nonEmpty))
  }
  
  import RxExecuterTest.JobOrderOps
  
  test("Single successful job") {
    /* Single-job pipeline:
     *
     *  Job1
     * 
     */

    val job1 = RxMockJob("Job_1")

    val executable = Executable(Set(job1))

    assert(job1.executionCount === 0)
    
    val ExecutionResults(results, chunks) = exec(executable)

    assert(job1.executionCount === 1)

    assert(results.size === 1)

    // Check if jobs were correctly chunked
    assert(chunks === Seq(Set(job1)))
    
    assert(results.values.head === JobState.Succeeded)
  }
  
  test("Single failed job") {
    /* Single-job pipeline:
     *
     *  Job1
     * 
     */
    def doTest(jobState: JobState): Unit = {
      val job1 = RxMockJob("Job_1", toReturn = jobState)
  
      assert(job1.executionCount === 0)
  
      val executable = Executable(Set(job1))
      
      val ExecutionResults(results, chunks) = exec(executable)
  
      assert(job1.executionCount === 1)
  
      assert(results.size === 1)
  
      // Check if jobs were correctly chunked
      assert(chunks === Seq(Set(job1)))
      
      assert(results.values.head === jobState)
    }
    
    doTest(JobState.Failed())
    doTest(JobState.FailedWithException(new Exception))
    doTest(JobState.CommandResult(42, LocalResources))
  }
  
  test("Two failed jobs") {
    /* Linear two-job pipeline:
     *
     *  Job1 --- Job2
     * 
     */
    def doTest(jobState: JobState): Unit = {

      val job1 = RxMockJob("Job_1", toReturn = jobState)
      val job2 = RxMockJob("Job_2", inputs = Set(job1), toReturn = jobState)
  
      assert(job1.executionCount === 0)
      assert(job2.executionCount === 0)
  
      val executable = Executable(Set(job1))
      
      val ExecutionResults(results, chunks) = exec(executable)
  
      //We expect that job wasn't run, since the preceding job failed
      assert(job1.executionCount === 1)
      assert(job2.executionCount === 0)
  
      assert(results.size === 1)
  
      // Check if jobs were correctly chunked
      assert(chunks === Seq(Set(job1)))
      
      assert(results(job1) === jobState)
      assert(results.get(job2).isEmpty)
    }
    
    doTest(JobState.Failed())
    doTest(JobState.FailedWithException(new Exception))
    doTest(JobState.CommandResult(42, LocalResources))
  }
  
  test("3-job linear pipeline works") {
    /* A 3-step pipeline:
     *
     * Job1 -- Job2 -- Job3
     * 
     */

    val job1 = RxMockJob("Job_1")
    val job2 = RxMockJob("Job_2", Set(job1))
    val job3 = RxMockJob("Job_3", Set(job2))

    assert(job1.executionCount === 0)
    assert(job2.executionCount === 0)
    assert(job3.executionCount === 0)

    val executable = Executable(Set(job3))
    
    val r @ ExecutionResults(result, chunks) = exec(executable)
    
    import r.jobExecutionSeq

    assert(job1.executionCount === 1)
    assert(job2.executionCount === 1)
    assert(job3.executionCount === 1)

    assert(result(job1).isSuccess)
    assert(result(job2).isSuccess)
    assert(result(job3).isSuccess)
    
    assert(result.size === 3)
    
    assert(jobExecutionSeq == Seq(Set(job1), Set(job2), Set(job3)))
    
    assert(job1 ranBefore job2)
    assert(job1 ranBefore job3)
    assert(job2 ranBefore job3)
  }
  
  test("3-job linear pipeline works if the middle job fails") {
    /* A 3-step pipeline:
     *
     * Job1 -- Job2 -- Job3
     * 
     */

    val job1 = RxMockJob("Job_1")
    val job2 = RxMockJob("Job_2", Set(job1), toReturn = JobState.CommandResult(2, LocalResources))
    val job3 = RxMockJob("Job_3", Set(job2))

    assert(job1.executionCount === 0)
    assert(job2.executionCount === 0)
    assert(job3.executionCount === 0)

    val executable = Executable(Set(job3))
    
    val r @ ExecutionResults(result, chunks) = exec(executable)
    
    import r.jobExecutionSeq

    assert(job1.executionCount === 1)
    assert(job2.executionCount === 1)
    assert(job3.executionCount === 0)

    assert(result(job1).isSuccess)
    assert(result(job2).isFailure)
    assert(result.get(job3).isEmpty)
    
    assert(result.size === 2)
    
    assert(jobExecutionSeq == Seq(Set(job1), Set(job2)))
    
    assert(job1 ranBefore job2)
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

    val job1 = RxMockJob("Job_1")
    val job2 = RxMockJob("Job_2")
    val job3 = RxMockJob("Job_3", Set(job1, job2))

    assert(job1.executionCount === 0)
    assert(job2.executionCount === 0)
    assert(job3.executionCount === 0)

    val executable = Executable(Set(job3))
    
    val r @ ExecutionResults(result, chunks) = exec(executable)
    
    import r.jobExecutionSeq

    assert(job1.executionCount === 1)
    assert(job2.executionCount === 1)
    assert(job3.executionCount === 1)

    assert(result(job1).isSuccess)
    assert(result(job2).isSuccess)
    assert(result(job3).isSuccess)
    
    assert(result.size === 3)
    
    // check that relationships are maintained 
    assert(job1 ranBefore job3)
    assert(job2 ranBefore job3)
    
    assert(chunks === Seq(Set(job1, job2), Set(job3)))
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
    
    val job1 = RxMockJob("Job_1")
    val job2 = RxMockJob("Job_2", Set(job1))
    val job3 = RxMockJob("Job_3", Set(job1))

    assert(job1.executionCount === 0)
    assert(job2.executionCount === 0)
    assert(job3.executionCount === 0)

    val executable = Executable(Set(job2, job3))
    
    val r @ ExecutionResults(results, chunks) = exec(executable)
    
    import r.jobExecutionSeq

    assert(job1.executionCount === 1)
    assert(job2.executionCount === 1)
    assert(job3.executionCount === 1)

    assert(results(job1).isSuccess)
    assert(results(job2).isSuccess)
    assert(results(job3).isSuccess)
    
    assert(results.size === 3)
    
    // check that relationships are maintained
    assert(job1 ranBefore job2)
    assert(job1 ranBefore job3)
    
    assert(chunks === Seq(Set(job1), Set(job2, job3)) )
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
    
    val job1 = RxMockJob("Job_1")
    val job2 = RxMockJob("Job_2", Set(job1))
    val job3 = RxMockJob("Job_3", Set(job1))
    val job4 = RxMockJob("Job_4", Set(job2, job3))

    assert(job1.executionCount === 0)
    assert(job2.executionCount === 0)
    assert(job3.executionCount === 0)
    assert(job4.executionCount === 0)

    val executable = Executable(Set(job4))
    
    val r @ ExecutionResults(results, _) = exec(executable)
    
    import r.jobExecutionSeq

    assert(Seq(job1, job2, job3, job4).map(_.executionCount) == Seq(1,1,1,1))

    assert(results(job1).isSuccess)
    assert(results(job2).isSuccess)
    assert(results(job3).isSuccess)
    assert(results(job4).isSuccess)
    
    assert(results.size === 4)
    
    // Only check that relationships are maintained, 
    // not for a literal sequence of chunks, since the latter is non-deterministic.
    assert(job1 ranBefore job2)
    assert(job1 ranBefore job3)
    
    assert(job2 ranBefore job4)
    assert(job3 ranBefore job4)
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

    val executable = Executable(Set(job4))
    
    val r @ ExecutionResults(result, _) = exec(executable)
    
    import r.jobExecutionSeq

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
    // Only check that relationships are maintained, 
    // not for a literal sequence of chunks, since the latter is non-deterministic.
    assert(job11 ranBefore job21)
    assert(job11 ranBefore job22)
    
    assert(job12 ranBefore job23)
    assert(job12 ranBefore job24)
    
    assert(job21 ranBefore job31)
    assert(job22 ranBefore job31)
    
    assert(job23 ranBefore job32)
    assert(job24 ranBefore job32)

    assert(job31 ranBefore job4)
    assert(job32 ranBefore job4)
  }

  test("New leaves are executed as soon as possible when initial jobs don't start simultaneously") {
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

    // The delay added to job11 should cause job23 and job24 to be bundled and executed prior to job21 and job22
    lazy val job11 = RxMockJob("Job_1_1", runsAfter = Set(job12), fakeExecutionTimeInMs = 500)
    lazy val job12 = RxMockJob("Job_1_2")
    lazy val job21 = RxMockJob("Job_2_1", Set(job11))
    lazy val job22 = RxMockJob("Job_2_2", Set(job11))
    lazy val job23 = RxMockJob("Job_2_3", Set(job12), runsAfter = Set(job31), fakeExecutionTimeInMs = 1000)
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

    val executable = Executable(Set(job4))
    
    val r @ ExecutionResults(result, _) = exec(executable)
    
    //NB: Chunks can be empty if no jobs became runnable during a certain period (RxExecuter.windowLength)
    //This can happen non-deterministically since the precise timing of when jobs will be run can't be known.
    //Filter out empty chunks here; we can still test that jobs were chunked properly and in the right order.  
    implicit val jobExecutionSeq = r.jobExecutionSeq.filterNot(_.isEmpty)

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
    assert(jobExecutionSeq(0) === Set(job11, job12))
    assert(jobExecutionSeq(1) === Set(job23, job24))
    assert(jobExecutionSeq(2) === Set(job21, job22))
    assert(jobExecutionSeq(3) === Set(job31))
    assert(jobExecutionSeq(4) === Set(job32))
    assert(jobExecutionSeq(5) === Set(job4))
    assert(jobExecutionSeq.length === 6)

    // Also check that relationships are maintained,
    assert(job11 ranBefore job21)
    assert(job11 ranBefore job22)
    
    assert(job12 ranBefore job23)
    assert(job12 ranBefore job24)
    
    assert(job21 ranBefore job31)
    assert(job22 ranBefore job31)
    
    assert(job23 ranBefore job32)
    assert(job24 ranBefore job32)

    assert(job31 ranBefore job4)
    assert(job32 ranBefore job4)
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

    // scalastyle:off method.length
    def doTest(maxSimultaneousJobs: Int): Unit = {
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
  
      assert(job11.executionCount === 0)
      assert(job12.executionCount === 0)
      assert(job21.executionCount === 0)
      assert(job22.executionCount === 0)
      assert(job23.executionCount === 0)
      assert(job24.executionCount === 0)
      assert(job25.executionCount === 0)
      assert(job31.executionCount === 0)
      assert(job32.executionCount === 0)
      assert(job4.executionCount === 0)
      
      val executable = Executable(Set(job4))
  
      import scala.concurrent.duration._
      
      val realRunner = AsyncLocalChunkRunner(maxSimultaneousJobs)
      
      assert(realRunner.maxNumJobs === maxSimultaneousJobs)
      
      val runner = MockChunkRunner(realRunner)
      
      assert(runner.maxNumJobs === maxSimultaneousJobs)
    
      val executer = RxExecuter(runner, 0.25.seconds, JobFilter.RunEverything)
      
      val result = executer.execute(executable)
      
      implicit val jobExecutionSeq = runner.chunks.value
  
      assert(job11.executionCount === 1)
      assert(job12.executionCount === 1)
      assert(job21.executionCount === 1)
      assert(job22.executionCount === 1)
      assert(job23.executionCount === 1)
      assert(job24.executionCount === 1)
      assert(job25.executionCount === 1)
      assert(job31.executionCount === 1)
      assert(job32.executionCount === 1)
      assert(job4.executionCount === 1)
      
      // Only check that relationships are maintained, 
      // not for a literal sequence of chunks, since the latter is non-deterministic.
      assert(job11 ranBefore job21)
      assert(job11 ranBefore job22)
    
      assert(job12 ranBefore job23)
      assert(job12 ranBefore job24)
      
      assert(job21 ranBefore job31)
      assert(job22 ranBefore job31)
      
      assert(job23 ranBefore job32)
      assert(job24 ranBefore job32)
      assert(job25 ranBefore job32)
  
      assert(job31 ranBefore job4)
      assert(job32 ranBefore job4)
      
      val allChunksWereRightSize = jobExecutionSeq.forall(_.size <= maxSimultaneousJobs)
      
      val msg = s"Expected all chunks to be <= $maxSimultaneousJobs big, but got $jobExecutionSeq"
      
      assertResult(true, msg)(allChunksWereRightSize)
    }
    
    doTest(4)
    doTest(8)
    doTest(1)
  }
  
  // scalastyle:on method.length
  // scalastyle:on magic.number
}

object RxExecuterTest {
  private final case class ExecutionResults(byJob: Map[LJob, JobState], chunks: Seq[Set[LJob]]) {
    implicit val jobExecutionSeq: Seq[Set[LJob]] = chunks
  }
  
  private final implicit class JobOrderOps(lhs: LJob)(implicit executionSeq: Seq[Set[LJob]]) {
    def ranBefore(rhs: LJob): Boolean = {
      val withIndices = executionSeq.zipWithIndex
      
      def indexOf(j: LJob): Int = {
        require(executionSeq.exists(_.contains(j)), s"Can't find job ${j.name} in $executionSeq")
        
        executionSeq.iterator.zipWithIndex.collect { case (jobs, i) if jobs.contains(j) => i }.next()
      }
      
      val lhsIndex = indexOf(lhs)
      val rhsIndex = indexOf(rhs)
      
      lhsIndex < rhsIndex
    }
  }
  
  private final case class MockChunkRunner(delegate: ChunkRunner) extends ChunkRunner {
    override def maxNumJobs: Int = delegate.maxNumJobs
    
    override def canRun(job: LJob): Boolean = delegate.canRun(job)
    
    val chunks: ValueBox[Seq[Set[LJob]]] = ValueBox(Vector.empty)

    override def run(jobs: Set[LJob]): Observable[Map[LJob, JobState]] = {
      chunks.mutate(_ :+ jobs)

      delegate.run(jobs)
    }
  }
}
