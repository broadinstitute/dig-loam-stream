package loamstream.uger

import org.scalatest.FunSuite

import scala.util.Success
import scala.concurrent.duration.Duration
import scala.concurrent.Await

import loamstream.util.ObservableEnrichments
import scala.concurrent.Future
import rx.lang.scala.schedulers.IOScheduler
import loamstream.util.RxSchedulers

/**
 * @author clint
 * date: Jul 6, 2016
 */
final class JobMonitorTest extends FunSuite {
  private def waitFor[A](f: Future[A]): A = Await.result(f, Duration.Inf) 
  
  test("stop()") {
    val (scheduler, handle) = RxSchedulers.backedByThreadPool(1)
    
    try {
      val client = MockDrmaaClient(Map.empty)
      
      val jobMonitor = new JobMonitor(scheduler, Poller.drmaa(client))
      
      assert(jobMonitor.isStopped === false)
      
      jobMonitor.stop()
      
      assert(jobMonitor.isStopped)
      
      jobMonitor.stop()
      
      assert(jobMonitor.isStopped)
    } finally { 
      handle.shutdown() 
    }
  }
  
  test("monitor() - happy path") {
    import JobStatus._
    
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
    import ObservableEnrichments._
    
    val poller = Poller.drmaa(client)
    
    val (scheduler, handle) = RxSchedulers.backedByThreadPool(3)
    
    try {
      val statuses = (new JobMonitor(scheduler, poller, 9.99)).monitor(jobIds)
    
      def futureStatuses(jobId: String): Future[Seq[JobStatus]] = statuses(jobId).to[Seq].firstAsFuture
    
      val fut1 = futureStatuses(jobId1)
      val fut2 = futureStatuses(jobId2)
      val fut3 = futureStatuses(jobId3)

      assert(waitFor(fut1) == Seq(Queued, Running, Done))
      assert(waitFor(fut2) == Seq(Running, Done))
      assert(waitFor(fut3) == Seq(Running, Done))
    } finally {
      handle.shutdown()
    }
  }
}
