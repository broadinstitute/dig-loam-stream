package loamstream.drm

import scala.util.Success

import scala.collection.immutable.Seq

import org.scalatest.FunSuite

import monix.execution.Scheduler

import loamstream.TestHelpers
import loamstream.TestHelpers.DummyDrmJobOracle
import loamstream.util.Schedulers
import loamstream.util.Observables


/**
 * @author clint
 * date: Jul 6, 2016
 */
final class JobMonitorTest extends FunSuite {
 

  test("monitor() - happy path") {
    import DrmStatus._
    
    val taskId1 = DrmTaskId("foo", 42)
    val taskId2 = DrmTaskId("bar", 42)
    val taskId3 = DrmTaskId("baz", 42)
    
    val jobIds = Seq(taskId1, taskId2, taskId3)
    
    val poller = MockPoller(
      Map(
        taskId1 -> Seq(Success(Queued), Success(Running), Success(Running), Success(Done)),
        taskId2 -> Seq(Success(Running), Success(Done)),
        taskId3 -> Seq(Success(Running), Success(Running), Success(Done))))
    
    import scala.concurrent.ExecutionContext.Implicits.global
    import Observables.Implicits._
    
    withThreadPoolScheduler(3) { scheduler =>
      def futureStatuses(taskId: DrmTaskId): Seq[DrmStatus] = {
        import monix.execution.Scheduler.Implicits.global
        
        val statuses = (new JobMonitor(scheduler, poller, 9.99)).monitor(DummyDrmJobOracle)(jobIds)
        
        statuses
          .collect { case (tid, status) if tid == taskId => status }
          .toListL
          .runSyncUnsafe(TestHelpers.defaultWaitTime)
      }
    
      val fut1 = futureStatuses(taskId1)
      val fut2 = futureStatuses(taskId2)
      val fut3 = futureStatuses(taskId3)

      assert(fut1 == Seq(Queued, Running, Running, Done))
      assert(fut2 == Seq(Running, Done))
      assert(fut3 == Seq(Running, Running, Done))
    }
  }
  
  test("stop") {
    val poller = MockPoller(Map.empty)
    
    //Doesn't matter which one, we won't run anything on it.
    val scheduler = Scheduler.computation()
    
    val jobMonitor = new JobMonitor(scheduler, poller, 9.99)

    assert(poller.isStopped === false)
      
    jobMonitor.stop()
      
    assert(poller.isStopped === true)
  }
  
  private def withThreadPoolScheduler[A](numThreads: Int)(f: Scheduler => A): A = {
    val (scheduler, handle) = Schedulers.backedByThreadPool(numThreads)
    
    try { f(scheduler) }
    finally { handle.stop() }
  }
}
