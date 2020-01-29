package loamstream.drm

import scala.collection.Seq
import scala.concurrent.Future
import scala.util.Success
import scala.util.Try

import org.scalatest.FunSuite

import loamstream.util.Observables
import loamstream.util.RxSchedulers
import loamstream.util.Tries

import rx.lang.scala.Scheduler


/**
 * @author clint
 * date: Jul 6, 2016
 */
final class JobMonitorTest extends FunSuite {
  import loamstream.TestHelpers.waitFor 

  test("getDrmStatusFor") {
    import JobMonitor.getDrmStatusFor
    
    val taskIdFoo = DrmTaskId("foo", 42)
    val taskIdBar = DrmTaskId("bar", 42)
    val taskIdBaz = DrmTaskId("baz", 42)
    
    assert(getDrmStatusFor(taskIdFoo)(Map.empty).isFailure)
    
    import DrmStatus.Done
    import DrmStatus.Running
    
    val failure: Try[DrmStatus] = Tries.failure("blerg")
    
    val attempts = Map(taskIdFoo -> Success(Done), taskIdBar -> Success(Running), taskIdBaz -> failure)
    
    assert(getDrmStatusFor(taskIdFoo)(attempts) === Success(Done))
    assert(getDrmStatusFor(taskIdBar)(attempts) === Success(Running))
    assert(getDrmStatusFor(taskIdBaz)(attempts) === failure)
    assert(getDrmStatusFor(DrmTaskId("asdgasdf", 42))(attempts).isFailure)
  }
  
  test("stop()") {
    withThreadPoolScheduler(1) { scheduler =>
      val client = MockDrmaaClient(Map.empty)
      
      val jobMonitor = new JobMonitor(scheduler, new DrmaaPoller(client))
      
      assert(jobMonitor.isStopped === false)
      
      jobMonitor.stop()
      
      assert(jobMonitor.isStopped)
      
      jobMonitor.stop()
      
      assert(jobMonitor.isStopped)
    }
  }
  
  test("monitor() - happy path") {
    import DrmStatus._
    
    val taskId1 = DrmTaskId("foo", 42)
    val taskId2 = DrmTaskId("bar", 42)
    val taskId3 = DrmTaskId("baz", 42)
    
    val jobIds = Seq(taskId1, taskId2, taskId3)
    
    val client = MockDrmaaClient(
      Map(
        taskId1 -> Seq(Success(Queued), Success(Running), Success(Running), Success(Done)),
        taskId2 -> Seq(Success(Running), Success(Done)),
        taskId3 -> Seq(Success(Running), Success(Running), Success(Done))))
    
    import scala.concurrent.ExecutionContext.Implicits.global
    import Observables.Implicits._
    
    val poller = new DrmaaPoller(client)
    
    withThreadPoolScheduler(3) { scheduler =>
      val statuses = (new JobMonitor(scheduler, poller, 9.99)).monitor(jobIds)
    
      def futureStatuses(taskId: DrmTaskId): Future[Seq[DrmStatus]] = statuses(taskId).to[Seq].firstAsFuture
    
      val fut1 = futureStatuses(taskId1)
      val fut2 = futureStatuses(taskId2)
      val fut3 = futureStatuses(taskId3)

      assert(waitFor(fut1) == Seq(Queued, Running, Done))
      assert(waitFor(fut2) == Seq(Running, Done))
      assert(waitFor(fut3) == Seq(Running, Done))
    }
  }
  
  private def withThreadPoolScheduler[A](numThreads: Int)(f: Scheduler => A): A = {
    val (scheduler, handle) = RxSchedulers.backedByThreadPool(numThreads)
    
    try { f(scheduler) }
    finally { handle.stop() }
  }
}
