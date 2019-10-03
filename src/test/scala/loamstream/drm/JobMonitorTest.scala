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
    
    assert(getDrmStatusFor("foo")(Map.empty).isFailure)
    
    import DrmStatus.Done
    import DrmStatus.Running
    
    val failure: Try[DrmStatus] = Tries.failure("blerg")
    
    val attempts = Map("foo" -> Success(Done), "bar" -> Success(Running), "baz" -> failure)
    
    assert(getDrmStatusFor("foo")(attempts) === Success(Done))
    assert(getDrmStatusFor("bar")(attempts) === Success(Running))
    assert(getDrmStatusFor("baz")(attempts) === failure)
    assert(getDrmStatusFor("asdgasdf")(attempts).isFailure)
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
    
    val jobId1 = "foo"
    val jobId2 = "bar"
    val jobId3 = "baz"
    
    val jobIds = Seq(jobId1, jobId2, jobId3)
    
    val client = MockDrmaaClient(
      Map(
        jobId1 -> Seq(Success(Queued), Success(Running), Success(Running), Success(Done)),
        jobId2 -> Seq(Success(Running), Success(Done)),
        jobId3 -> Seq(Success(Running), Success(Running), Success(Done))))
    
    import scala.concurrent.ExecutionContext.Implicits.global
    import Observables.Implicits._
    
    val poller = new DrmaaPoller(client)
    
    withThreadPoolScheduler(3) { scheduler =>
      val statuses = (new JobMonitor(scheduler, poller, 9.99)).monitor(jobIds)
    
      def futureStatuses(jobId: String): Future[Seq[DrmStatus]] = statuses(jobId).to[Seq].firstAsFuture
    
      val fut1 = futureStatuses(jobId1)
      val fut2 = futureStatuses(jobId2)
      val fut3 = futureStatuses(jobId3)

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
