package loamstream.model.execute

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.RxMockJob
import loamstream.util.ValueBox
import rx.lang.scala.Observable

/**
 * @author kyuksel
 *         date: Aug 17, 2016
 */
final class RxExecuterTest extends FunSuite {
  import RxExecuterTest.ExecutionResults
  import scala.concurrent.ExecutionContext.Implicits.global
  
  private def exec(
      jobs: Set[RxMockJob],
      maxRestarts: Int,
      maxSimultaneousJobs: Int = 8): ExecutionResults = {
    
    import scala.concurrent.duration._
    
    val runner = MockChunkRunner(AsyncLocalChunkRunner(maxSimultaneousJobs))
    
    val executer = RxExecuter(runner, 0.1.seconds, JobFilter.RunEverything, maxRunsPerJob = maxRestarts + 1)
    
    ExecutionResults(
        executer.execute(Executable(jobs.asInstanceOf[Set[LJob]])), 
        runner.chunks.value.filter(_.nonEmpty).map(_.asInstanceOf[Set[RxMockJob]]))
  }
  
  test("shouldRestart") {
    val job = MockJob(JobStatus.NotStarted)
    
    import RxExecuter.shouldRestart
    
    assert(job.runCount === 0)
    
    assert(shouldRestart(job, 0) === false)
    assert(shouldRestart(job, 1) === true)
    assert(shouldRestart(job, 42) === true)
    
    job.transitionTo(JobStatus.Running)
    
    assert(job.runCount === 1)
    
    assert(shouldRestart(job, 0) === false)
    assert(shouldRestart(job, 1) === false)
    assert(shouldRestart(job, 2) === true)
    assert(shouldRestart(job, 42) === true)
  }
  
  test("Guards") {
    import scala.concurrent.duration._
    
    val runner = MockChunkRunner(AsyncLocalChunkRunner(8))
    
    intercept[Exception] {
      RxExecuter(runner, 0.25.seconds, JobFilter.RunEverything, -1)
    }
    
    intercept[Exception] {
      RxExecuter(runner, 0.25.seconds, JobFilter.RunEverything, 0)
    }
    
    intercept[Exception] {
      RxExecuter(runner, 0.25.seconds, JobFilter.RunEverything, -100)
    }
    
    RxExecuter(runner, 0.25.seconds, JobFilter.RunEverything, 1)
    RxExecuter(runner, 0.25.seconds, JobFilter.RunEverything, 42)
  }

  import RxExecuterTest.JobOrderOps
  
  test("Single successful job") {
    /* Single-job pipeline:
     *
     *  Job1
     * 
     */
    def doTest(maxRestartsAllowed: Int): Unit = {
      val job1 = RxMockJob("Job_1")
  
      assert(job1.executionCount === 0)
      
      val ExecutionResults(results, chunks) = exec(Set(job1), maxRestartsAllowed)
  
      assert(job1.executionCount === 1)
  
      assert(results.size === 1)
  
      // Check if jobs were correctly chunked
      assert(chunks === Seq(Set(job1)))
      
      assert(results.values.head.status === JobStatus.Succeeded)
    }
    
    doTest(0)
    doTest(4)
    doTest(100)
  }
  
  test("Single failed job") {
    /* Single-job pipeline:
     *
     *  Job1
     * 
     */
    def doTest(maxRestartsAllowed: Int, expectedRuns: Int, jobResult: JobResult): Unit = {
      val job1 = RxMockJob("Job_1", toReturn = () => jobResult)

      assert(job1.executionCount === 0)

      val ExecutionResults(executions, chunks) = exec(Set(job1), maxRestartsAllowed)

      assert(job1.executionCount === expectedRuns)

      assert(executions.size === 1)

      // Check if jobs were correctly chunked; we should see job1 as many times as we expect it to be run
      val expectedChunks = (0 until expectedRuns).map(_ => Set(job1))
      
      assert(chunks === expectedChunks)

      assert(executions.values.head.result === Some(jobResult))
    }

    val failure = JobResult.Failure
    val failureWithException = JobResult.FailureWithException(new Exception)
    val commandResult = JobResult.CommandResult(42)
    
    doTest(maxRestartsAllowed = 0, expectedRuns = 1, jobResult = JobResult.Failure)
    doTest(maxRestartsAllowed = 0, expectedRuns = 1, jobResult = failureWithException)
    doTest(maxRestartsAllowed = 0, expectedRuns = 1, jobResult = commandResult)
    
    doTest(maxRestartsAllowed = 1, expectedRuns = 2, jobResult = JobResult.Failure)
    doTest(maxRestartsAllowed = 1, expectedRuns = 2, jobResult = failureWithException)
    doTest(maxRestartsAllowed = 1, expectedRuns = 2, jobResult = commandResult)
  }
  
  test("Single failed job, succeeds after 2 retries") {
    /* Single-job pipeline:
     *
     *  Job1
     * 
     */
    def doTest(maxRestartsAllowed: Int, expectedRuns: Int, shouldUltimatelyFail: Boolean): Unit = {
      val job1 = RxMockJob("Job_1", toReturn = RxExecuterTest.succeedsAfterNRuns(3))

      assert(job1.executionCount === 0)

      val ExecutionResults(executions, chunks) = exec(Set(job1), maxRestartsAllowed)

      assert(job1.executionCount === expectedRuns)

      assert(executions.size === 1)

      // Check if jobs were correctly chunked; we should see job1 as many times as we expect it to be run
      val expectedChunks = (0 until expectedRuns).map(_ => Set(job1))
      
      assert(chunks === expectedChunks)

      val expectedResult = if(shouldUltimatelyFail) JobResult.Failure else JobResult.Success
      
      assert(executions.values.head.result === Some(expectedResult))
    }

    doTest(maxRestartsAllowed = 0, expectedRuns = 1, shouldUltimatelyFail = true)
    doTest(maxRestartsAllowed = 1, expectedRuns = 2, shouldUltimatelyFail = true)
    doTest(maxRestartsAllowed = 2, expectedRuns = 3, shouldUltimatelyFail = false)
    doTest(maxRestartsAllowed = 4, expectedRuns = 3, shouldUltimatelyFail = false)
  }

  test("Two failed jobs") {
    /* Linear two-job pipeline:
     *
     *  Job1 --- Job2
     *
     */
    def doTest(maxRestartsAllowed: Int, expectedRuns: Seq[Int], jobResult: JobResult): Unit = {

      val job1 = RxMockJob("Job_1", toReturn = () => jobResult)
      val job2 = RxMockJob("Job_2", inputs = Set(job1), toReturn = () => jobResult)

      assert(job1.executionCount === 0)
      assert(job2.executionCount === 0)

      val ExecutionResults(executions, chunks) = exec(Set(job1), maxRestartsAllowed)

      //We expect that job wasn't run, since the preceding job failed
      val Seq(expectedRuns1, expectedRuns2) = expectedRuns
      
      assert(job1.executionCount === expectedRuns1)
      assert(job2.executionCount === expectedRuns2)

      assert(executions.size === 1)

      // Check if jobs were correctly chunked; we should see job1 as many times as we expect it to be run.
      // We shouldn't see job2, since we shouldn't ever try to run it since job1 fails.
      val expectedChunks = (0 until expectedRuns1).map(_ => Set(job1))
      
      assert(chunks === expectedChunks)

      assert(executions(job1) === TestHelpers.executionFromResult(jobResult))
      assert(executions.get(job2).isEmpty)
    }

    doTest(maxRestartsAllowed = 0, expectedRuns = Seq(1,0), jobResult = JobResult.Failure)
    doTest(maxRestartsAllowed = 0, expectedRuns = Seq(1,0), jobResult = JobResult.FailureWithException(new Exception))
    doTest(maxRestartsAllowed = 0, expectedRuns = Seq(1,0), jobResult = JobResult.CommandResult(42))
    
    doTest(maxRestartsAllowed = 2, expectedRuns = Seq(3,0), jobResult = JobResult.Failure)
    doTest(maxRestartsAllowed = 2, expectedRuns = Seq(3,0), jobResult = JobResult.FailureWithException(new Exception))
    doTest(maxRestartsAllowed = 2, expectedRuns = Seq(3,0), jobResult = JobResult.CommandResult(42))
  }

  test("3-job linear pipeline works") {
    /* A 3-step pipeline:
     *
     * Job1 -- Job2 -- Job3
     *
     */
    def doTest(maxRestartsAllowed: Int): Unit = {
      implicit val executionsBox: ValueBox[Vector[RxMockJob]] = ValueBox(Vector.empty)
      
      val job1 = RxMockJob("Job_1")
      val job2 = RxMockJob("Job_2", Set(job1))
      val job3 = RxMockJob("Job_3", Set(job2))
  
      assert(job1.executionCount === 0)
      assert(job2.executionCount === 0)
      assert(job3.executionCount === 0)
  
      val ExecutionResults(result, chunks) = exec(Set(job3), maxRestartsAllowed)
  
      assert(job1.executionCount === 1)
      assert(job2.executionCount === 1)
      assert(job3.executionCount === 1)
  
      assert(result(job1).isSuccess)
      assert(result(job2).isSuccess)
      assert(result(job3).isSuccess)
  
      assert(result.size === 3)
  
      assert(chunks == Seq(Set(job1), Set(job2), Set(job3)))
  
      job1 assertRanBefore job2
      job1 assertRanBefore job3
      job2 assertRanBefore job3
    }
    
    doTest(0)
    doTest(2)
  }

  test("3-job linear pipeline works if the middle job fails") {
    /* A 3-step pipeline:
     *
     * Job1 -- Job2 -- Job3
     *
     */
    def doTest(maxRestartsAllowed: Int, expectedRuns: Seq[Int]): Unit = {
      implicit val executionsBox: ValueBox[Vector[RxMockJob]] = ValueBox(Vector.empty)
      
      val job1 = RxMockJob("Job_1")
      val job2 = RxMockJob("Job_2", Set(job1), toReturn = () => JobResult.CommandResult(2))
      val job3 = RxMockJob("Job_3", Set(job2))
  
      assert(job1.executionCount === 0)
      assert(job2.executionCount === 0)
      assert(job3.executionCount === 0)
  
      val ExecutionResults(result, chunks) = exec(Set(job3), maxRestartsAllowed)
  
      val Seq(expectedRuns1, expectedRuns2, expectedRuns3) = expectedRuns
      
      assert(job1.executionCount === expectedRuns1)
      assert(job2.executionCount === expectedRuns2)
      assert(job3.executionCount === expectedRuns3)
  
      assert(result(job1).isSuccess)
      assert(result(job2).isFailure)
      assert(result.get(job3).isEmpty)
      
      assert(result.size === 2)
      
      val expectedChunks: Seq[Set[RxMockJob]] = Set(job1) +: (0 until expectedRuns2).toSeq.map(_ => Set(job2))
      
      assert(chunks === expectedChunks)
      
      job1 assertRanBefore job2
    }
    
    doTest(maxRestartsAllowed = 0, expectedRuns = Seq(1,1,0))
    doTest(maxRestartsAllowed = 2, expectedRuns = Seq(1,3,0))
  }
  
  test("3-job linear pipeline works if the middle job fails but then succeeds after a restart") {
    /* A 3-step pipeline:
     *
     * Job1 -- Job2 -- Job3
     *
     */
    def doTest(maxRestartsAllowed: Int, expectedRuns: Seq[Int]): Unit = {
      
      implicit val executionsBox: ValueBox[Vector[RxMockJob]] = ValueBox(Vector.empty)
      
      val job1 = RxMockJob("Job_1")
      val job2 = RxMockJob("Job_2", Set(job1), toReturn = RxExecuterTest.succeedsAfterNRuns(3))
      val job3 = RxMockJob("Job_3", Set(job2))
  
      assert(job1.executionCount === 0)
      assert(job2.executionCount === 0)
      assert(job3.executionCount === 0)
      
      val ExecutionResults(result, chunks) = exec(Set(job3), maxRestartsAllowed)
      
      val Seq(expectedRuns1, expectedRuns2, expectedRuns3) = expectedRuns  
      
      assert(job1.executionCount === expectedRuns1)
      assert(job2.executionCount === expectedRuns2)
      assert(job3.executionCount === expectedRuns3)
  
      assert(result(job1).isSuccess)
      
      val job3ShouldFail = expectedRuns3 == 0
      
      if(job3ShouldFail) {
        //If we expect job3 not to have run, in other words, we expect job2 to fail 
        assert(result(job2).isFailure)
        assert(result.get(job3).isEmpty)
      } else {
        //Otherwise, we expect job2 and job3 to work
        assert(result(job2).isSuccess)
        assert(result(job3).isSuccess)
      }
      
      val expectedNumResults = if(job3ShouldFail) 2 else 3
        
      assert(result.size === expectedNumResults)
      
      val expectedChunks = {
        val expectedChunksForJob2 = (0 until expectedRuns2).toSeq.map(_ => Set(job2))
      
        val expectedChunksForJobs1And2 = Set(job1) +: expectedChunksForJob2
        
        if(job3ShouldFail) { expectedChunksForJobs1And2 } 
        else { expectedChunksForJobs1And2 :+ Set(job3) }
      }

      assert(chunks === expectedChunks)  
      
      job1 assertRanBefore job2
    }
    
    doTest(maxRestartsAllowed = 0, expectedRuns = Seq(1,1,0))
    doTest(maxRestartsAllowed = 2, expectedRuns = Seq(1,3,1))
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
    def doTest(maxRestartsAllowed: Int): Unit = {
      implicit val executionsBox: ValueBox[Vector[RxMockJob]] = ValueBox(Vector.empty)
      
      val job1 = RxMockJob("Job_1")
      val job2 = RxMockJob("Job_2")
      val job3 = RxMockJob("Job_3", Set(job1, job2))
  
      assert(job1.executionCount === 0)
      assert(job2.executionCount === 0)
      assert(job3.executionCount === 0)
  
      val ExecutionResults(result, chunks) = exec(Set(job3), maxRestartsAllowed)
  
      assert(job1.executionCount === 1)
      assert(job2.executionCount === 1)
      assert(job3.executionCount === 1)
  
      assert(result(job1).isSuccess)
      assert(result(job2).isSuccess)
      assert(result(job3).isSuccess)
      
      assert(result.size === 3)
      
      // check that relationships are maintained 
      job1 assertRanBefore job3
      job2 assertRanBefore job3
      
      //NB: Don't check for specific chunks, since those are non-deterministic.
    }
    
    doTest(0)
    doTest(2)
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
    def doTest(maxRestartsAllowed: Int): Unit = {
      implicit val executionsBox: ValueBox[Vector[RxMockJob]] = ValueBox(Vector.empty)
      
      val job1 = RxMockJob("Job_1")
      val job2 = RxMockJob("Job_2", Set(job1))
      val job3 = RxMockJob("Job_3", Set(job1))
  
      assert(job1.executionCount === 0)
      assert(job2.executionCount === 0)
      assert(job3.executionCount === 0)

      val ExecutionResults(results, chunks) = exec(Set(job2, job3), maxRestartsAllowed)
  
      assert(job1.executionCount === 1)
      assert(job2.executionCount === 1)
      assert(job3.executionCount === 1)
  
      assert(results(job1).isSuccess)
      assert(results(job2).isSuccess)
      assert(results(job3).isSuccess)
      
      assert(results.size === 3)
      
      // check that relationships are maintained
      job1 assertRanBefore job2
      job1 assertRanBefore job3
      
      assert(chunks === Seq(Set(job1), Set(job2, job3)))
    }
    
    doTest(0)
    //doTest(2)
    
    
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
    def doTest(maxRestartsAllowed: Int): Unit = {
      implicit val executionsBox: ValueBox[Vector[RxMockJob]] = ValueBox(Vector.empty)
      
      val job1 = RxMockJob("Job_1")
      val job2 = RxMockJob("Job_2", Set(job1))
      val job3 = RxMockJob("Job_3", Set(job1))
      val job4 = RxMockJob("Job_4", Set(job2, job3))
  
      assert(Seq(job1, job2, job3, job4).map(_.executionCount) == Seq(0,0,0,0))
  
      val ExecutionResults(results, _) = exec(Set(job4), maxRestartsAllowed)
  
      assert(Seq(job1, job2, job3, job4).map(_.executionCount) == Seq(1,1,1,1))
  
      assert(results(job1).isSuccess)
      assert(results(job2).isSuccess)
      assert(results(job3).isSuccess)
      assert(results(job4).isSuccess)
      
      assert(results.size === 4)
      
      // Only check that relationships are maintained, 
      // not for a literal sequence of chunks, since the latter is non-deterministic.
      job1 assertRanBefore job2
      job1 assertRanBefore job3
      
      job2 assertRanBefore job4
      job3 assertRanBefore job4
    }
    
    doTest(0)
    doTest(2)
  }
  
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
    def doTest(maxRestartsAllowed: Int): Unit = {
      implicit val executionsBox: ValueBox[Vector[RxMockJob]] = ValueBox(Vector.empty)
      
      val job11 = RxMockJob("Job_1_1")
      val job12 = RxMockJob("Job_1_2")
      val job21 = RxMockJob("Job_2_1", Set(job11))
      val job22 = RxMockJob("Job_2_2", Set(job11))
      val job23 = RxMockJob("Job_2_3", Set(job12))
      val job24 = RxMockJob("Job_2_4", Set(job12))
      val job31 = RxMockJob("Job_3_1", Set(job21, job22))
      val job32 = RxMockJob("Job_3_2", Set(job23, job24))
      val job4 = RxMockJob("Job_4", Set(job31, job32))
  
      def assertExecutionCounts(expected: Int): Unit = {
        assert(job11.executionCount === expected)
        assert(job12.executionCount === expected)
        assert(job21.executionCount === expected)
        assert(job22.executionCount === expected)
        assert(job23.executionCount === expected)
        assert(job24.executionCount === expected)
        assert(job31.executionCount === expected)
        assert(job32.executionCount === expected)
        assert(job4.executionCount === expected)
      }
      
      assertExecutionCounts(0)
  
      val ExecutionResults(result, _) = exec(Set(job4), maxRestartsAllowed)
      
      assertExecutionCounts(1)
  
      assert(result.size === 9)
  
      // Check if jobs were correctly chunked
      // Only check that relationships are maintained, 
      // not for a literal sequence of chunks, since the latter is non-deterministic.
      job11 assertRanBefore job21
      job11 assertRanBefore job22
      
      job12 assertRanBefore job23
      job12 assertRanBefore job24
      
      job21 assertRanBefore job31
      job22 assertRanBefore job31
      
      job23 assertRanBefore job32
      job24 assertRanBefore job32
  
      job31 assertRanBefore job4
      job32 assertRanBefore job4
    }
    
    doTest(0)
    doTest(2)
  }

  //TODO: Assess the usefulness of this test.  It's caused a lot of problems, both because of the initial 
  //implementation, and because of various fixes and workarounds in RxMockJob and this class.  It's also not clear
  //what the original intent of this test was. 
  test("New leaves are executed as soon as possible when some sub-trees run before others") {
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
    def doTest(maxRestartsAllowed: Int): Unit = {
      implicit val executionsBox: ValueBox[Vector[RxMockJob]] = ValueBox(Vector.empty)
      
      // Ensure Job23 and Job24 run after job31
      lazy val job11 = RxMockJob("Job_1_1")
      lazy val job12 = RxMockJob("Job_1_2")
      lazy val job21 = RxMockJob("Job_2_1", Set(job11))
      lazy val job22 = RxMockJob("Job_2_2", Set(job11))
      lazy val job23 = RxMockJob("Job_2_3", Set(job12), runsAfter = Some(job31))
      lazy val job24 = RxMockJob("Job_2_4", Set(job12), runsAfter = Some(job31))
      lazy val job31 = RxMockJob("Job_3_1", Set(job21, job22))
      lazy val job32 = RxMockJob("Job_3_2", Set(job23, job24))
      lazy val job4 = RxMockJob("Job_4", Set(job31, job32))
      
      def assertExecutionCounts(expected: Int): Unit = {
        Seq(job11, job12, job21, job22, job23, job24, job31, job32, job4).foreach { job =>
          assert(job.executionCount === expected, s"Expected $expected runs from $job, but got ${job.executionCount}")
        }
      }
      
      assertExecutionCounts(0)
      
      //NB: Make sure the full stack trace of any exceptions is logged/printed, since SBT truncates them.
      val ExecutionResults(result, _) = RxExecuterTest.logFullStackTrace {
        exec(Set(job4), maxRestartsAllowed)
      }
  
      assertExecutionCounts(1)
  
      assert(result.size === 9)
  
      // NB: Don't check for specific chunks, since this is non-deterministic now that RxExecuter samples
      // The stream of runnables with Observable.tumbling.
  
      // Check that relationships are maintained,
      job11 assertRanBefore job21 ; job11 assertRanBefore job22
      
      job12 assertRanBefore job23 ; job12 assertRanBefore job24
      
      job21 assertRanBefore job31 ; job22 assertRanBefore job31
      
      job23 assertRanBefore job31 ; job24 assertRanBefore job31
      job23 assertRanBefore job32 ; job24 assertRanBefore job32
  
      job31 assertRanBefore job32 
      
      job31 assertRanBefore job4 ; job32 assertRanBefore job4
    }
    
    doTest(0)
    doTest(2)
  }
  
  private def executeWithMockRunner(
      maxSimultaneousJobs: Int, 
      maxRestartsAllowed: Int,
      jobs: Set[LJob]): (MockChunkRunner, Map[LJob, Execution]) = {
    
    import scala.concurrent.duration._
      
    val realRunner = AsyncLocalChunkRunner(maxSimultaneousJobs)
  
    assert(realRunner.maxNumJobs === maxSimultaneousJobs)
    
    val runner = MockChunkRunner(realRunner)
      
    assert(runner.maxNumJobs === maxSimultaneousJobs)
    
    val executer = RxExecuter(runner, 0.1.seconds, JobFilter.RunEverything, maxRestartsAllowed + 1)
    
    (runner, executer.execute(Executable(jobs)))
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
    def doTest(maxRestartsAllowed: Int, maxSimultaneousJobs: Int): Unit = {
      implicit val executionsBox: ValueBox[Vector[RxMockJob]] = ValueBox(Vector.empty)
      
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
  
      def assertExecutionCount(expected: Int): Unit = {
        Seq(job11, job12, job21, job22, job23, job24, job25, job31, job32, job4).foreach { job =>
          assert(job.executionCount === expected, s"Expected $expected runs for $job, but got ${job.executionCount}")
        }
      }
      
      assertExecutionCount(0)
      
      val (runner, result) = executeWithMockRunner(maxSimultaneousJobs, maxRestartsAllowed, Set(job4))
      
      val jobExecutionSeq = runner.chunks.value.map(_.asInstanceOf[Set[RxMockJob]])
  
      assertExecutionCount(1)
      
      // Only check that relationships are maintained, 
      // not for a literal sequence of chunks, since the latter is non-deterministic.
      job11 assertRanBefore job21 ; job11 assertRanBefore job22
    
      job12 assertRanBefore job23 ; job12 assertRanBefore job24
      
      job21 assertRanBefore job31 ; job22 assertRanBefore job31
      
      job23 assertRanBefore job32 ; job24 assertRanBefore job32 ; job25 assertRanBefore job32
  
      job31 assertRanBefore job4 ; job32 assertRanBefore job4
      
      val allChunksWereRightSize = jobExecutionSeq.forall(_.size <= maxSimultaneousJobs)
      
      assert(
          allChunksWereRightSize === true, 
          s"Expected all chunks to be <= $maxSimultaneousJobs big, but got $jobExecutionSeq")
    }
    
    doTest(maxRestartsAllowed = 0, maxSimultaneousJobs = 4)
    doTest(maxRestartsAllowed = 0, maxSimultaneousJobs = 8)
    doTest(maxRestartsAllowed = 0, maxSimultaneousJobs = 1)
    
    doTest(maxRestartsAllowed = 2, maxSimultaneousJobs = 4)
    doTest(maxRestartsAllowed = 2, maxSimultaneousJobs = 8)
    doTest(maxRestartsAllowed = 2, maxSimultaneousJobs = 1)
  }
}

object RxExecuterTest {
  private def logFullStackTrace[A](f: => A): A = {
    try { f }
    catch {
      case scala.util.control.NonFatal(e) => {
        e.printStackTrace()
        
        throw e
      }
    }
  }
  
  private def succeedsAfterNRuns(n: Int): () => JobResult = { 
    val runs: ValueBox[Int] = ValueBox(0)
    
    () => {
      runs.mutate(_ + 1)
        
      if(runs.value >= n) JobResult.Success else JobResult.Failure
    }
  }
  
  private final case class ExecutionResults(byJob: Map[LJob, Execution], chunks: Seq[Set[RxMockJob]])
  
  private final implicit class JobOrderOps(lhs: RxMockJob) {
    def assertRanBefore(rhs: RxMockJob)(implicit executionsBox: ValueBox[Vector[RxMockJob]]): Unit = {
      val executions = executionsBox.value
      
      val lhsIndex = executions.indexOf(lhs)
      val rhsIndex = executions.indexOf(rhs)
      
      assert(
        lhsIndex < rhsIndex, 
        s"lhs index ($lhsIndex) not < rhs index ($rhsIndex) in $executions")
    }
  }
  
  private final case class MockChunkRunner(delegate: ChunkRunner) extends ChunkRunner {
    override def maxNumJobs: Int = delegate.maxNumJobs
    
    override def canRun(job: LJob): Boolean = delegate.canRun(job)
    
    val chunks: ValueBox[Seq[Set[LJob]]] = ValueBox(Vector.empty)

    override def run(jobs: Set[LJob], shouldRestart: LJob => Boolean): Observable[Map[LJob, Execution]] = {
      chunks.mutate(_ :+ jobs)

      delegate.run(jobs, shouldRestart)
    }
  }
}
