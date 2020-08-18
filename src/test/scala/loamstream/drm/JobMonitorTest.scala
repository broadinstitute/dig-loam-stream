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
  import Observables.Implicits._ 

  test("getDrmStatusFor") {
    import JobMonitor.getDrmStatusFor
    
    val taskIdFoo = DrmTaskId("foo", 42)
    val taskIdBar = DrmTaskId("bar", 42)
    val taskIdBaz = DrmTaskId("baz", 42)
    
    assert(waitFor(getDrmStatusFor(taskIdFoo)(Map.empty).isEmpty.firstAsFuture) === true)
    
    import DrmStatus.Done
    import DrmStatus.Running
    
    val failure: Try[DrmStatus] = Tries.failure("blerg")
    
    val attempts = Map(taskIdFoo -> Success(Done), taskIdBar -> Success(Running), taskIdBaz -> failure)
    
    assert(waitFor(getDrmStatusFor(taskIdFoo)(attempts).firstAsFuture) === Success(Done))
    assert(waitFor(getDrmStatusFor(taskIdBar)(attempts).firstAsFuture) === Success(Running))
    assert(waitFor(getDrmStatusFor(taskIdBaz)(attempts).firstAsFuture) === failure)
    assert(waitFor(getDrmStatusFor(DrmTaskId("asdgasdf", 42))(attempts).isEmpty.firstAsFuture) === true)
  }
  
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
      val statuses = (new JobMonitor(scheduler, poller, 9.99)).monitor(jobIds)
    
      def futureStatuses(taskId: DrmTaskId): Future[Seq[DrmStatus]] = {
        statuses.collect { case (tid, status) if tid == taskId => status }.to[Seq].firstAsFuture
      }
    
      val fut1 = futureStatuses(taskId1)
      val fut2 = futureStatuses(taskId2)
      val fut3 = futureStatuses(taskId3)

      assert(waitFor(fut1) == Seq(Queued, Running, Running, Done))
      assert(waitFor(fut2) == Seq(Running, Done))
      assert(waitFor(fut3) == Seq(Running, Running, Done))
    }
  }
  
  private def withThreadPoolScheduler[A](numThreads: Int)(f: Scheduler => A): A = {
    val (scheduler, handle) = RxSchedulers.backedByThreadPool(numThreads)
    
    try { f(scheduler) }
    finally { handle.stop() }
  }
}
