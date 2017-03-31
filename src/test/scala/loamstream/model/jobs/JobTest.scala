package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.util.Futures
import loamstream.util.ObservableEnrichments

import scala.concurrent.ExecutionContext

/**
 * @author clint
 * date: May 27, 2016
 */
final class JobTest extends FunSuite with TestJobs {
  
  //scalastyle:off magic.number
  
  import JobStatus._
  import Futures.waitFor
  import ObservableEnrichments._

  val failedJob = MockJob(Failed)

  test("execute") {
    val statuses = failedJob.statuses.until(_.isFinished).to[Seq].firstAsFuture

    failedJob.execute(ExecutionContext.global)
    
    assert(waitFor(statuses) === Seq(NotStarted, Running, Failed))
  }
  
  test("lastStatus - simple") {
    val lastStatusFuture = failedJob.lastStatus.firstAsFuture

    failedJob.execute(ExecutionContext.global)
    
    assert(waitFor(lastStatusFuture) === Failed)
  }

  test("lastStatus - subsequent 'terminal' Statuses don't count") {
    val lastStatusesFuture = failedJob.lastStatus.to[Seq].firstAsFuture

    failedJob.updateAndEmitJobStatus(NotStarted)
    failedJob.updateAndEmitJobStatus(NotStarted)
    failedJob.updateAndEmitJobStatus(Running)
    failedJob.updateAndEmitJobStatus(Running)
    failedJob.updateAndEmitJobStatus(Failed)

    assert(waitFor(lastStatusesFuture) === Seq(Failed))
  }
  
  test("finalInputStatuses - no deps") {
    val noDeps = failedJob
    
    val finalInputStatusesFuture = noDeps.finalInputStatuses.firstAsFuture
    
    assert(waitFor(finalInputStatusesFuture) === Nil)
  }
  
  test("finalInputStatuses - some deps") {
    val deps: Set[LJob] = Set(MockJob(Failed), MockJob(Succeeded))
    
    val noDeps = MockJob(toReturn = Failed, inputs = deps)
    
    val finalInputStatusesFuture = noDeps.finalInputStatuses.firstAsFuture
    
    deps.foreach(_.execute(ExecutionContext.global))
    
    //NB: Use Sets to ignore order
    val expected = Set(Failed, Succeeded)
    
    assert(waitFor(finalInputStatusesFuture).toSet === expected)
  }
  
  test("state/statuses/updateAndEmitJobState") {
    val first5Statuses = failedJob.statuses.take(5).to[Seq].firstAsFuture
    
    assert(failedJob.status === NotStarted)
    
    failedJob.updateAndEmitJobStatus(Unknown)
    
    assert(failedJob.status === Unknown)
    
    failedJob.updateAndEmitJobStatus(Failed)
    
    assert(failedJob.status === Failed)
    
    failedJob.updateAndEmitJobStatus(Running)
    
    assert(failedJob.status === Running)
    
    failedJob.updateAndEmitJobStatus(FailedWithException)
    
    assert(failedJob.status === FailedWithException)
    
    failedJob.updateAndEmitJobStatus(Succeeded)
    
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
      
      assert(waitFor(noDeps.selfRunnable.firstAsFuture) eq noDeps)
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
        
        j.updateAndEmitJobStatus(startingStatus.getOrElse(toReturn))
        
        j
      }
      
      val notFinished = mockJob(Succeeded, startingStatus = Some(Running))
      
      val i0 = mockJob(Succeeded)
      
      val i1 = mockJob(if(anyFailures) Failed else Succeeded)
      
      val inputs: Set[LJob] = Set(i0, notFinished, i1)
      
      val job = MockJob(toReturn = resultStatus, inputs = inputs)

      notFinished.updateAndEmitJobStatus(Succeeded)
      
      if(anyFailures) {
        assert(waitFor(job.selfRunnable.isEmpty.firstAsFuture))
      } else {
        val selfRunnableFuture = job.selfRunnable.firstAsFuture
        
        assert(waitFor(selfRunnableFuture) eq job)
      }
    }
    
    doTest(Succeeded, anyFailures = true)
    doTest(Succeeded, anyFailures = false)
    doTest(Failed, anyFailures = true)
    doTest(Failed, anyFailures = false)
    doTest(NotStarted, anyFailures = true)
    doTest(NotStarted, anyFailures = false)
    doTest(FailedWithException, anyFailures = true)
    doTest(FailedWithException, anyFailures = false)
    doTest(Unknown, anyFailures = true)
    doTest(Unknown, anyFailures = false)
  }
  
  test("runnables - no deps") {
    def doTest(resultStatus: JobStatus): Unit = {
      val job = MockJob(resultStatus)
      
      val runnables = job.runnables.to[Seq].firstAsFuture
      
      assert(waitFor(runnables) === Seq(job))
      
      assert(waitFor(runnables).head eq job)
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
  
  test("runnables - some deps, no failures") {
    
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
    
    def execute(jobs: Iterable[LJob]): Unit = jobs.foreach(_.execute(ExecutionContext.global))

    val gc0 = MockJob(Succeeded)
    val gc1 = MockJob(Skipped)
    val gc2 = MockJob(Succeeded)
    val gc3 = MockJob(Skipped)
    
    val c0 = MockJob(Succeeded, inputs = Set[LJob](gc0, gc1))
    val c1 = MockJob(Succeeded, inputs = Set[LJob](gc2, gc3))
    
    val rootJob = MockJob(Succeeded, inputs = Set[LJob](c0,c1))
    
    val grandChildren = waitFor(rootJob.runnables.take(4).to[Set].firstAsFuture)
    
    assert(grandChildren === Set(gc0, gc1, gc2, gc3))
    
    val futureChildren = rootJob.runnables.drop(4).take(2).to[Set].firstAsFuture
    
    execute(grandChildren)
    
    assert(waitFor(futureChildren) === Set(c0, c1))
    
    execute(Seq(c0, c1))
    
    val futureRoot = rootJob.runnables.drop(6).to[Set].firstAsFuture
    
    val roots = waitFor(futureRoot)
    
    assert(roots === Set(rootJob))
    assert(roots.head eq rootJob)
  }
  
  test("runnables - some deps, some failures") {
    
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
    
    def execute(jobs: Iterable[LJob]): Unit = jobs.foreach(_.execute(ExecutionContext.global))
    
    val gc0 = MockJob(Succeeded)
    val gc1 = MockJob(Skipped)
    val gc2 = MockJob(Succeeded)
    val gc3 = MockJob(Skipped)
    
    val c0 = MockJob(toReturn = Failed, inputs = Set[LJob](gc0, gc1))
    val c1 = MockJob(toReturn = Succeeded, inputs = Set[LJob](gc2, gc3))
    
    val rootJob = MockJob(Succeeded, inputs = Set[LJob](c0,c1))
    
    val grandChildren = waitFor(rootJob.runnables.take(4).to[Set].firstAsFuture)
    
    //We should get all the grandchildren, since they start out runnable
    assert(grandChildren === Set(gc0, gc1, gc2, gc3))
    
    val futureChildren = rootJob.runnables.drop(4).take(2).to[Set].firstAsFuture
    
    execute(grandChildren)
    
    //We should get all the children, since their children all succeed
    assert(waitFor(futureChildren) === Set(c0, c1))
    
    execute(Seq(c0, c1))
    
    //We shouldn't get the root, since one of its children failed
    val futureRootMissing = rootJob.runnables.drop(6).isEmpty.firstAsFuture
    
    assert(waitFor(futureRootMissing))
  }
  
  //scalastyle:on magic.number
}
