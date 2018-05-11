package loamstream.uger

import scala.concurrent.Await
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import org.scalatest.FunSuite

import loamstream.util.ObservableEnrichments
import scala.util.Try
import loamstream.drm.DrmStatus
import loamstream.drm.DrmaaPoller

/**
 * @author clint
 * date: Jul 6, 2016
 */
final class PollerTest extends FunSuite {
  import DrmStatus._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._
  import ObservableEnrichments._
  
  //TODO: Replace with Futures.waitFor
  private def waitFor[A](f: Future[A]): A = Await.result(f, Duration.Inf)

  private val zero = Duration.Zero
  
  test("drama().poll() - happy path") {
    val jobId = "foo"
    
    val client = MockDrmaaClient(Map(jobId -> Seq(Success(Running), Success(Running), Success(Done))))
    
    val poller = new DrmaaPoller(client)
    
    val statuses = Seq(
      poller.poll(Seq(jobId)),
      poller.poll(Seq(jobId)),
      poller.poll(Seq(jobId)))
    
    val Seq(s0, s1, s2) = statuses.map(_.values.head)
    
    assert(s0 == Success(Running))
    assert(s1 == Success(Running))
    assert(s2 == Success(Done))
      
    assert(client.params.value == Seq(jobId -> zero, jobId -> zero, jobId -> zero))
  }
  
  test("drmaa().poll() - happy path, multiple jobs") {
    val jobId1 = "foo"
    val jobId2 = "bar"
    val jobId3 = "baz"
    
    val jobIds = Seq(jobId1, jobId2, jobId3)
    
    val client = MockDrmaaClient(
      Map(
        jobId1 -> Seq(Success(Running), Success(Running), Success(Running), Success(Done)),
        jobId2 -> Seq(Success(Running), Success(Done)),
        jobId3 -> Seq(Success(Running), Success(Running), Success(Done))))
    
    val poller = new DrmaaPoller(client)
  
    def poll: Map[String, Try[DrmStatus]] = poller.poll(jobIds)
    
    {
      val expected = Map(
          jobId1 -> Success(Running),
          jobId2 -> Success(Running),
          jobId3 -> Success(Running))
          
      assert(poll == expected)
    }
    
    {
      val expected = Map(
          jobId1 -> Success(Running),
          jobId2 -> Success(Done),
          jobId3 -> Success(Running))
          
      assert(poll == expected)
    }
    
    {
      val expected = Map(
          jobId1 -> Success(Running),
          jobId2 -> Success(Done),
          jobId3 -> Success(Done))
          
      assert(poll == expected)
    }
    
    {
      val expected = Map(
          jobId1 -> Success(Done),
          jobId2 -> Success(Done),
          jobId3 -> Success(Done))
          
      assert(poll == expected)
    }
    
    {
      val expected = Map(
          jobId1 -> Success(Done),
          jobId2 -> Success(Done),
          jobId3 -> Success(Done))
          
      assert(poll == expected)
    }
  }
  
  test("drmaa().poll() - some failures") {
    val exception = new Exception with scala.util.control.NoStackTrace
    
    val jobId = "foo"
    
    val jobIds = Seq(jobId)
    
    val client = MockDrmaaClient(
      Map(
        jobId -> Seq(Success(Running), Failure(exception), Success(Done), Failure(exception))))
    
    val poller = new DrmaaPoller(client)
    
    val results = Seq(
      poller.poll(jobIds),
      poller.poll(jobIds),
      poller.poll(jobIds),
      poller.poll(jobIds)).map(_.values.head)
     
    
    val Seq(s0, s1, s2, s3) = results 
    
    assert(s0 == Success(Running))
    assert(s1 == Failure(exception))
    assert(s2 == Success(Done))
    assert(s3 == Failure(exception))
      
    assert(client.params.value == Seq(jobId -> zero, jobId -> zero, jobId -> zero, jobId -> zero))
  }
}
