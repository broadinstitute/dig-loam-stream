package loamstream.model.execute

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.model.jobs.RxMockJob
import loamstream.util.ValueBox
import monix.reactive.Observable
import scala.collection.compat._


/**
 * @author kyuksel
 *         date: Aug 17, 2016
 */
//TODO:
//scalastyle:off file.size.limit
final class RxExecuterTest extends FunSuite {
  import RxExecuterTest.ExecutionResults
  import RxExecuterTest.JobOrderOps
  import scala.concurrent.ExecutionContext.Implicits.global
  
  private def makeExecuter(maxRestarts: Int, maxSimultaneousJobs: Int = 8): (RxExecuter, MockChunkRunner) = {
    import scala.concurrent.duration._
    
    val runner = MockChunkRunner(AsyncLocalChunkRunner(ExecutionConfig.default, maxSimultaneousJobs))
    
    import RxExecuter.Defaults.fileMonitor
    
    val executer = RxExecuter(
        RxExecuter.Defaults.executionConfig,
        runner, 
        fileMonitor, 
        0.1.seconds, 
        JobCanceler.NeverCancel,
        JobFilter.RunEverything, 
        ExecutionRecorder.DontRecord, 
        maxRunsPerJob = maxRestarts + 1)
        
    (executer, runner)
  }
  
  private def doExec(executer: RxExecuter, runner: MockChunkRunner, jobs: Set[RxMockJob]): ExecutionResults = {
    ExecutionResults(
        executer.execute(Executable(jobs.asInstanceOf[Set[JobNode]])), 
        runner.chunks.value.filter(_.nonEmpty).map(_.asInstanceOf[Set[RxMockJob]]))
  }
  
  private def exec(
      jobs: Set[RxMockJob],
      maxRestarts: Int,
      maxSimultaneousJobs: Int = 8): ExecutionResults = {
    
    val (executer, runner) = makeExecuter(maxRestarts, maxSimultaneousJobs)
    
    doExec(executer, runner, jobs)
  }
  
  test("3-job pipeline where one job is canceled before running") {
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
      
      lazy val job1: RxMockJob = RxMockJob(s"Job_1", successors = () => Set(job3))
      lazy val job2: RxMockJob = RxMockJob(s"Job_2", successors = () => Set(job3))
      lazy val job3: RxMockJob = RxMockJob(s"Job_3", Set(job1, job2))
  
      assert(job1.executionCount === 0)
      assert(job2.executionCount === 0)
      assert(job3.executionCount === 0)
  
      val (executer, runner) = makeExecuter(maxRestartsAllowed)
      
      val willCancelJob2: JobCanceler = new JobCanceler {
        override def shouldCancel(job: LJob): Boolean = job eq job2
      }
      
      val mungedExecutor = executer.copy(jobCanceler = willCancelJob2)
      
      val ExecutionResults(result, _) = doExec(mungedExecutor, runner, Set(job3))
  
      assert(job1.executionCount === 1)
      assert(job2.executionCount === 0)
      assert(job3.executionCount === 0)
  
      assert(result(job1).isSuccess === true)
      assert(result(job2).isFailure === false)
      assert(result(job2).isSuccess === false)
      assert(result.get(job3) === None)
      
      assert(result(job2).status === JobStatus.Canceled)
      
      assert(result(job2).result === None)
      
      assert(result.size === 2)
    }
    
    doTest(0)
    doTest(2)
  }
  
  test("Guards") {
    import scala.concurrent.duration._
    
    val executionConfig = ExecutionConfig.default
    
    val runner = MockChunkRunner(AsyncLocalChunkRunner(executionConfig, 8))
    
    import RxExecuter.Defaults.fileMonitor
    
    val jobCanceler = JobCanceler.NeverCancel
    val jobFilter = JobFilter.RunEverything
    val executionRecorder = ExecutionRecorder.DontRecord
    
    intercept[Exception] {
      RxExecuter(executionConfig, runner, fileMonitor, 0.25.seconds, jobCanceler, jobFilter, executionRecorder, -1)
    }
    
    intercept[Exception] {
      RxExecuter(executionConfig, runner, fileMonitor, 0.25.seconds, jobCanceler, jobFilter, executionRecorder, 0)
    }
    
    intercept[Exception] {
      RxExecuter(executionConfig, runner, fileMonitor, 0.25.seconds, jobCanceler, jobFilter, executionRecorder, -100)
    }
    
    RxExecuter(executionConfig, runner, fileMonitor, 0.25.seconds, jobCanceler, jobFilter, executionRecorder, 1)
    RxExecuter(executionConfig, runner, fileMonitor, 0.25.seconds, jobCanceler, jobFilter, executionRecorder, 42)
  }

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
    
    doTest(maxRestartsAllowed = 0, expectedRuns = 1, jobResult = failure)
    doTest(maxRestartsAllowed = 0, expectedRuns = 1, jobResult = failureWithException)
    doTest(maxRestartsAllowed = 0, expectedRuns = 1, jobResult = commandResult)
    
    doTest(maxRestartsAllowed = 1, expectedRuns = 2, jobResult = failure)
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

      val expectedResult: JobResult = if(shouldUltimatelyFail) JobResult.Failure else JobResult.Success
      
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
      val job2 = RxMockJob("Job_2", dependencies = Set(job1), toReturn = () => jobResult)

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
      
      lazy val job1: RxMockJob = RxMockJob("Job_1", successors = () => Set(job2))
      lazy val job2: RxMockJob = {
        RxMockJob("Job_2", Set(job1), toReturn = () => JobResult.CommandResult(2), successors = () => Set(job3))
      }
      lazy val job3: RxMockJob = RxMockJob("Job_3", Set(job2))
  
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
      
      val expectedChunks: Seq[Set[RxMockJob]] = Set(job1) +: (0 until expectedRuns2).to(Seq).map(_ => Set(job2))
      
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
      
      lazy val job1: RxMockJob = RxMockJob("Job_1", successors = () => Set(job2))
      lazy val job2: RxMockJob = {
        RxMockJob("Job_2", Set(job1), toReturn = RxExecuterTest.succeedsAfterNRuns(3), successors = () => Set(job3))
      }
      lazy val job3: RxMockJob = RxMockJob("Job_3", Set(job2))
  
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
        val expectedChunksForJob2 = (0 until expectedRuns2).to(Seq).map(_ => Set(job2))
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
  
  private def executeWithMockRunner(
      maxSimultaneousJobs: Int, 
      maxRestartsAllowed: Int,
      jobs: Set[LJob]): (MockChunkRunner, Map[LJob, Execution]) = {
    
    import scala.concurrent.duration._
      
    val realRunner = AsyncLocalChunkRunner(ExecutionConfig.default, maxSimultaneousJobs)
    
    val runner = MockChunkRunner(realRunner)
    
    import RxExecuter.Defaults.fileMonitor
    
    val executer = {
      RxExecuter(
          RxExecuter.Defaults.executionConfig,
          runner, 
          fileMonitor, 
          0.1.seconds, 
          JobCanceler.NeverCancel,
          JobFilter.RunEverything, 
          ExecutionRecorder.DontRecord, 
          maxRestartsAllowed + 1)
    }
    
    (runner, executer.execute(Executable(jobs.asInstanceOf[Set[JobNode]])))
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
    override def canRun(job: LJob): Boolean = delegate.canRun(job)
    
    val chunks: ValueBox[Seq[Iterable[LJob]]] = ValueBox(Vector.empty)

    override def run(
        jobs: Iterable[LJob], 
        jobOracle: JobOracle): Observable[(LJob, RunData)] = {
      
      chunks.mutate(_ :+ jobs)

      delegate.run(jobs, jobOracle)
    }
  }
}
//scalastyle:on file.size.limit
