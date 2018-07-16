package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.ExecutionConfig
import loamstream.model.execute.AsyncLocalChunkRunner

/**
 * @author clint
 * date: May 27, 2016
 */
final class JobTest extends FunSuite with TestJobs {
  
  import JobStatus._
  import loamstream.TestHelpers.waitFor
  import loamstream.util.Observables.Implicits._

  private def count[A](as: Seq[A]): Map[A, Int] = as.groupBy(identity).mapValues(_.size)
  
  //TODO: Lame :(
  private def toLJob(lj: LocalJob): LJob = lj
  //TODO: Lame :(
  private def toJobNode(j: LJob): JobNode = j
  //TODO: Lame :(
  private def toLocalJob(j: LJob): LocalJob = j.asInstanceOf[LocalJob]
  
  test("transitionTo") {
    val job = MockJob(NotStarted)
    
    def statuses(howMany: Int): Seq[JobStatus] = waitFor(job.statuses.take(howMany).to[Seq].firstAsFuture)
    
    assert(job.status === NotStarted)
    assert(job.runCount === 0)    
    
    job.transitionTo(Running)
    
    assert(job.status === Running)
    assert(statuses(1) === Seq(Running))
    assert(job.runCount === 1)
    
    job.transitionTo(Failed)
    assert(job.status === Failed)
    job.transitionTo(Running)
    
    assert(job.status === Running)
    assert(statuses(3) === Seq(Running, Failed, Running))
    assert(job.runCount === 2)
    
    //NB: Transitioning to the state we're already at should have no effect
    job.transitionTo(Running)
    job.transitionTo(Running)
    job.transitionTo(Running)
    job.transitionTo(Running)
    
    assert(job.status === Running)
    assert(statuses(3) === Seq(Running, Failed, Running))
    assert(job.runCount === 2)
    
    job.transitionTo(Failed)
    assert(job.status === Failed)
    
    //NB: Transitioning to the state we're already at should have no effect
    job.transitionTo(Failed)
    job.transitionTo(Failed)
    job.transitionTo(Failed)
    job.transitionTo(Failed)
    assert(job.status === Failed)
    
    job.transitionTo(Running)
    
    assert(job.status === Running)
    assert(statuses(5) === Seq(Running, Failed, Running, Failed, Running))
    assert(job.runCount === 3)
  }
  
  test("execute") {
    def doTest(status: JobStatus): Unit = {
      val failedJob = MockJob(status)
    
      val executionFuture = failedJob.execute(ExecutionContext.global)
    
      assert(waitFor(executionFuture).jobStatus === status)
    }
    
    doTest(JobStatus.Failed)
    doTest(JobStatus.FailedWithException)
    doTest(JobStatus.Skipped)
    doTest(JobStatus.Succeeded)
  }
  
  test("lastStatus - simple") {
    val runner = new AsyncLocalChunkRunner(ExecutionConfig.default)(ExecutionContext.global)

    def run(job: MockJob) = runner.run(Set(job), TestHelpers.neverRestart)
    
    def doTest(terminalStatus: JobStatus): Unit = {
      assert(terminalStatus.isTerminal)
    
      val terminalJob = MockJob(terminalStatus)
    
      val lastStatusFuture = terminalJob.lastStatus.firstAsFuture

      run(terminalJob)
    
      assert(waitFor(lastStatusFuture) === terminalStatus)
    }
    
    doTest(Succeeded)
    doTest(FailedPermanently)
    doTest(Skipped)
  }

  test("lastStatus - subsequent 'terminal' Statuses don't count") {
    val failedJob = MockJob(Failed)
    
    val lastStatusesFuture = failedJob.lastStatus.to[Seq].firstAsFuture

    failedJob.transitionTo(Running)
    failedJob.transitionTo(Failed)
    failedJob.transitionTo(FailedPermanently)
    failedJob.transitionTo(Succeeded)

    assert(waitFor(lastStatusesFuture) === Seq(FailedPermanently))
  }
  
  test("finalInputStatuses - no deps") {
    val failedJob = MockJob(Failed)
    
    val noDeps = failedJob
    
    val finalInputStatusesFuture = noDeps.finalInputStatuses.firstAsFuture
    
    assert(waitFor(finalInputStatusesFuture) === Nil)
  }
  
  test("finalInputStatuses - some deps") {
    val runner = new AsyncLocalChunkRunner(ExecutionConfig.default)(ExecutionContext.global)

    def run(jobs: Set[LJob]) = runner.run(jobs, TestHelpers.neverRestart)
    
    val deps: Set[LocalJob] = Set(MockJob(FailedPermanently), MockJob(Succeeded))
    
    //TODO: Lame :(
    val depsAsLJobs: Set[LJob] = deps.map(toLJob)
    
    //TODO: Lame :(
    val depsAsJobNodes: Set[JobNode] = deps.map(toJobNode)
    
    val noDeps = MockJob(toReturn = Failed, inputs = depsAsJobNodes)
    
    val finalInputStatusesFuture = noDeps.finalInputStatuses.firstAsFuture
    
    run(depsAsLJobs)
    
    //NB: Use Sets to ignore order
    val expected = Set(FailedPermanently, Succeeded)
    
    assert(waitFor(finalInputStatusesFuture).toSet === expected)
  }
  
  test("statuses/transitionTo") {
    val failedJob = MockJob(Failed)
    
    val first5Statuses = failedJob.statuses.take(5).to[Seq].firstAsFuture
    
    assert(failedJob.status === NotStarted)
    
    failedJob.transitionTo(Unknown)
    
    assert(failedJob.status === Unknown)
    
    failedJob.transitionTo(Failed)
    
    assert(failedJob.status === Failed)
    
    failedJob.transitionTo(Running)
    
    assert(failedJob.status === Running)
    
    failedJob.transitionTo(FailedWithException)
    
    assert(failedJob.status === FailedWithException)
    
    failedJob.transitionTo(Succeeded)
    
    assert(failedJob.status === Succeeded)
    
    val expected = Seq(
        Unknown, 
        Failed,
        Running,
        FailedWithException,
        Succeeded)
    
    assert(waitFor(first5Statuses) === expected)
  }
  
  test("selfRunnable - no deps") {
    def doTest(resultStatus: JobStatus): Unit = {
      val noDeps = MockJob(resultStatus)
      
      assert(waitFor(noDeps.selfRunnables.map(_.job).firstAsFuture) eq noDeps)
    }
    
    doTest(Succeeded)
    doTest(Failed)
    doTest(NotStarted)
    doTest(FailedWithException)
    doTest(Unknown)
    doTest(Terminated)
    doTest(Unknown)
    doTest(Submitted)
    doTest(Running)
    doTest(Skipped)
  }
  
  test("selfRunnable - some deps") {
    def doTest(resultStatus: JobStatus, anyFailures: Boolean): Unit = {
      def mockJob(toReturn: JobStatus, startingStatus: Option[JobStatus] = None) = {
        val j = MockJob(toReturn)
        
        j.transitionTo(startingStatus.getOrElse(toReturn))
        
        j
      }
      
      val notFinished = mockJob(Succeeded, startingStatus = Some(Running))
      
      val i0 = mockJob(Succeeded)
      
      val i1 = mockJob(if(anyFailures) FailedPermanently else Succeeded)
      
      val inputs: Set[JobNode] = Set(i0, notFinished, i1)
      
      val job = MockJob(toReturn = resultStatus, inputs = inputs)

      notFinished.transitionTo(Succeeded)
      
      if(anyFailures) {
        val jobRun = waitFor(job.selfRunnables.firstAsFuture)
        assert(jobRun.job eq job)
        assert(jobRun.status === CouldNotStart)
      } else {
        val selfRunnableFuture = job.selfRunnables.map(_.job).firstAsFuture
        
        assert(waitFor(selfRunnableFuture) eq job)
      }
    }
    
    doTest(Succeeded, anyFailures = true)
    doTest(Succeeded, anyFailures = false)
    doTest(FailedPermanently, anyFailures = true)
    doTest(FailedPermanently, anyFailures = false)
    doTest(NotStarted, anyFailures = true)
    doTest(NotStarted, anyFailures = false)
    doTest(Failed, anyFailures = true)
    doTest(Failed, anyFailures = false)
    doTest(FailedWithException, anyFailures = true)
    doTest(FailedWithException, anyFailures = false)
    doTest(Unknown, anyFailures = true)
    doTest(Unknown, anyFailures = false)
  }
  
  test("runnables - no deps") {
    val runner = new AsyncLocalChunkRunner(ExecutionConfig.default)(ExecutionContext.global)

    def run(job: LJob) = runner.run(Set(job), TestHelpers.neverRestart)
    
    def doTest(resultStatus: JobStatus): Unit = {
      val job = MockJob(resultStatus)
      
      val runnables: Future[Seq[JobRun]] = {
        if(resultStatus.isTerminal) { job.runnables.to[Seq].firstAsFuture }
        else { job.runnables.take(1).to[Seq].firstAsFuture }
      }
      
      run(job)
      
      assert(waitFor(runnables).map(_.job) === Seq(job))
      
      assert(waitFor(runnables).head.job eq job)
    }
    
    doTest(Succeeded)
    doTest(Failed)
    doTest(FailedWithException)
    doTest(Unknown)
    doTest(Terminated)
    doTest(Unknown)
    doTest(Submitted)
    doTest(Running)
    doTest(Skipped)
  }
  
  test("runnables - some deps, no failures") {
    
    val runner = new AsyncLocalChunkRunner(ExecutionConfig.default)(ExecutionContext.global)

    def run(jobs: LJob*) = runner.run(jobs.toSet, TestHelpers.neverRestart)
    
    /*
     * gc0
     *    \    
     *     +---c0 
     *    /      \       
     * gc1        \
     *             +---root
     * gc2        /
     *    \      /
     *     +---c1
     *    /
     * gc3
     */
    val gc0: LJob = MockJob(Succeeded)
    val gc1: LJob = MockJob(Skipped)
    val gc2 = MockJob(Succeeded)
    val gc3 = MockJob(Skipped)
    
    val c0 = MockJob(Succeeded, inputs = Set[JobNode](gc0, gc1))
    val c1 = MockJob(Succeeded, inputs = Set[JobNode](gc2, gc3))
    
    val rootJob = MockJob(Succeeded, inputs = Set[JobNode](c0,c1))
    
    val grandChildren = waitFor(rootJob.runnables.map(_.job).take(4).to[Set].firstAsFuture)
    
    assert(grandChildren === Set(gc0, gc1, gc2, gc3))
    
    val futureChildren = rootJob.runnables.map(_.job).drop(4).take(2).to[Set].firstAsFuture
    
    //TODO: Lame cast :(
    run(grandChildren.toSeq.map(toLocalJob): _*)
    
    run(c0, c1)
    
    run(rootJob)
    
    assert(waitFor(futureChildren) === Set(c0, c1))
    
    val futureRoot = rootJob.runnables.map(_.job).drop(6).to[Set].firstAsFuture
    
    val roots = waitFor(futureRoot)
    
    assert(roots === Set(rootJob))
    assert(roots.head eq rootJob)
  }
  
  test("runnables - some deps, some failures") {
    
    val runner = new AsyncLocalChunkRunner(ExecutionConfig.default)(ExecutionContext.global)

    def run(jobs: LJob*) = runner.run(jobs.toSet, TestHelpers.alwaysRestart)
    
    /*
     * gc0 (success)
     *    \    
     *     +-------c0 (failure)
     *    /         \       
     * gc1 (success) \
     *                +---root
     * gc2 (success) /
     *    \         /
     *     +-------c1 (success)
     *    /
     * gc3 (success)
     */
    val gc0 = MockJob(Succeeded, "gc0")
    val gc1 = MockJob(Skipped, "gc1")
    val gc2 = MockJob(Succeeded, "gc2")
    val gc3 = MockJob(Skipped, "gc3")
    
    val c0 = MockJob(toReturn = Failed, inputs = Set[JobNode](gc0, gc1), name = "c0")
    val c1 = MockJob(toReturn = Succeeded, inputs = Set[JobNode](gc2, gc3), name = "c1")
    
    val rootJob = MockJob(Succeeded, inputs = Set[JobNode](c0,c1), name = "root")
    
    val grandChildren = waitFor(rootJob.runnables.map(_.job).take(4).to[Seq].firstAsFuture)
    
    //We should get all the grandchildren, since they start out runnable
    assert(grandChildren.toSet === Set(gc0, gc1, gc2, gc3))
    
    val futureChildren = rootJob.runnables.map(_.job).drop(4).take(2).to[Seq].firstAsFuture
    
    //TODO: Lame cast :(
    run(grandChildren.map(toLocalJob): _*)
    
    //We should get all the children, since their children all succeed
    //NB: We should get c0 once here, since it won't become runnable until we execute it, and it fails
    assert(waitFor(futureChildren).toSet === Set(c0, c1))
    
    run(c0, c1)
    
    val futureNonRootRunnables = rootJob.runnables.map(_.job).take(7).to[Seq].firstAsFuture
    
    val nonRootRunnables = waitFor(futureNonRootRunnables)
    
    val counts = count(nonRootRunnables)
    
    assert(counts(gc0) === 1)
    assert(counts(gc1) === 1)
    assert(counts(gc2) === 1)
    assert(counts(gc3) === 1)
    //We expect to see c0 twice: once when it became runnable because its grandchildren finished, once when it 
    //became eligible for restarting after it failed.
    assert(counts(c0) === 2)
    assert(counts(c1) === 1)
    //We should not have seen the root, since one of its children failed
    assert(counts.get(rootJob) === None) 
    
    //We shouldn't get the root, since one of its children failed
    
    val futureRoot = rootJob.runnables.drop(7).firstAsFuture
    
    //Make c0 fail permanently, simulating a ChunkRunner/Executer deciding not to restart it.
    //We need to do this so c0.runnables completes, so that rootJob.selfRunnable will be computed.
    c0.transitionTo(FailedPermanently)
    
    val jobRun = waitFor(futureRoot)
    
    assert(jobRun.job eq rootJob)
    assert(jobRun.status === CouldNotStart)
  }
  
  test("One job, multiple failures, ultimately succeeds") {
    val runner = new AsyncLocalChunkRunner(ExecutionConfig.default)(ExecutionContext.global)

    def run(jobs: LJob*) = runner.run(jobs.toSet, TestHelpers.alwaysRestart)
    
    val job = MockJob(Failed, "job")
    
    val firstRunnable = job.runnables.map(_.job).take(1).firstAsFuture
    
    //The job should be runnable right away, since it has no deps
    assert(waitFor(firstRunnable) === job)
    
    val firstAndSecond = job.runnables.map(_.job).take(2).to[Seq].firstAsFuture
    
    run(job)
    
    //After the job runs (and fails), we expect 2 runnables: once for the job initially (since it had no deps)
    //and once for the failure
    assert(waitFor(firstAndSecond) === Seq(job, job))
    
    val firstSecondAndThird = job.runnables.map(_.job).take(3).to[Seq].firstAsFuture
    
    run(job)
    
    //After the job runs twice (and fails twice), we expect 3 runnables: once for the job initially (since it had 
    //no deps) and once for each of the two failures.
    assert(waitFor(firstSecondAndThird) === Seq(job, job, job))
    
    val futureRunnables = job.runnables.map(_.job).to[Seq].firstAsFuture
    
    //Make the job complete successfully
    job.transitionTo(Succeeded)
    
    //Now, we should be able to get all the runnables from this job, without limiting with take(), since 
    //runnables will have completed due to the terminal status.  Note that no more jobs are emitted from
    //runnables due to the terminal status.
    assert(waitFor(futureRunnables) === Seq(job, job, job))
  }
  
  test("One job, multiple failures, ultimately fails") {
    val runner = new AsyncLocalChunkRunner(ExecutionConfig.default)(ExecutionContext.global)

    def run(jobs: LJob*) = runner.run(jobs.toSet, TestHelpers.alwaysRestart)
    
    val job = MockJob(Failed, "job")
    
    val firstRunnable = job.runnables.map(_.job).take(1).firstAsFuture
    
    //The job should be runnable right away, since it has no deps
    assert(waitFor(firstRunnable) === job)
    
    val firstAndSecond = job.runnables.map(_.job).take(2).to[Seq].firstAsFuture
    
    run(job)
    
    //After the job runs (and fails), we expect 2 runnables: once for the job initially (since it had no deps)
    //and once for the failure
    assert(waitFor(firstAndSecond) === Seq(job, job))
    
    val firstSecondAndThird = job.runnables.map(_.job).take(3).to[Seq].firstAsFuture
    
    run(job)
    
    //After the job runs twice (and fails twice), we expect 3 runnables: once for the job initially (since it had 
    //no deps) and once for each of the two failures.
    assert(waitFor(firstSecondAndThird) === Seq(job, job, job))
    
    val futureRunnables = job.runnables.map(_.job).to[Seq].firstAsFuture
    
    //Make the job complete successfully
    job.transitionTo(FailedPermanently)
    
    //Now, we should be able to get all the runnables from this job, without limiting with take(), since 
    //runnables will have completed due to the terminal status.  Note that no more jobs are emitted from
    //runnables due to the terminal status.
    assert(waitFor(futureRunnables) === Seq(job, job, job))
  }
}
