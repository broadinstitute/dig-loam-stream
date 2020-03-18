package loamstream.drm

import scala.collection.Seq
import scala.concurrent.Await
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.util.Observables
import rx.lang.scala.Observable


/**
 * @author clint
 * date: Jul 6, 2016
 */
final class DrmaaPollerTest extends FunSuite {
  import DrmStatus._
  import scala.concurrent.duration._
  
  private def waitFor[A](f: Future[A]): A = TestHelpers.waitFor(f)

  private val zero = Duration.Zero

  import Observables.Implicits._
  
  test("drama().poll() - happy path") {
    val taskId = DrmTaskId("foo", 42)
    
    val client = MockDrmaaClient(Map(taskId -> Seq(Success(Running), Success(Running), Success(Done))))
    
    val poller = new DrmaaPoller(client)
    
    val statuses = Seq(
      poller.poll(Seq(taskId)),
      poller.poll(Seq(taskId)),
      poller.poll(Seq(taskId)))
    
    val Seq(s0, s1, s2) = statuses.map(_.map { case (_, status) => status }.firstAsFuture).map(waitFor)
    
    assert(s0 == Success(Running))
    assert(s1 == Success(Running))
    assert(s2 == Success(Done))
      
    assert(client.params.value == Seq(taskId -> zero, taskId -> zero, taskId -> zero))
  }
  
  test("drmaa().poll() - happy path, multiple jobs") {
    val taskId1 = DrmTaskId("foo", 42)
    val taskId2 = DrmTaskId("bar", 43)
    val taskId3 = DrmTaskId("baz", 44)
    
    val taskIds = Seq(taskId1, taskId2, taskId3)
    
    val client = MockDrmaaClient(
      Map(
        taskId1 -> Seq(Success(Running), Success(Running), Success(Running), Success(Done)),
        taskId2 -> Seq(Success(Running), Success(Done)),
        taskId3 -> Seq(Success(Running), Success(Running), Success(Done))))
    
    val poller = new DrmaaPoller(client)
  
    def poll: Map[DrmTaskId, Try[DrmStatus]] = waitFor(poller.poll(taskIds).toSeq.map(_.toMap).firstAsFuture)
    
    {
      val expected = Map(
          taskId1 -> Success(Running),
          taskId2 -> Success(Running),
          taskId3 -> Success(Running))
          
      assert(poll == expected)
    }
    
    {
      val expected = Map(
          taskId1 -> Success(Running),
          taskId2 -> Success(Done),
          taskId3 -> Success(Running))
          
      assert(poll == expected)
    }
    
    {
      val expected = Map(
          taskId1 -> Success(Running),
          taskId2 -> Success(Done),
          taskId3 -> Success(Done))
          
      assert(poll == expected)
    }
    
    {
      val expected = Map(
          taskId1 -> Success(Done),
          taskId2 -> Success(Done),
          taskId3 -> Success(Done))
          
      assert(poll == expected)
    }
    
    {
      val expected = Map(
          taskId1 -> Success(Done),
          taskId2 -> Success(Done),
          taskId3 -> Success(Done))
          
      assert(poll == expected)
    }
  }
  
  test("drmaa().poll() - some failures") {
    val exception = new Exception with scala.util.control.NoStackTrace
    
    val taskId = DrmTaskId("foo", 42)
    
    val taskIds = Seq(taskId)
    
    val client = MockDrmaaClient(
      Map(
        taskId -> Seq(Success(Running), Failure(exception), Success(Done), Failure(exception))))
    
    val poller = new DrmaaPoller(client)
    
    def poll() = waitFor(poller.poll(taskIds).toSeq.map(_.toMap).firstAsFuture)
    
    val results = Seq(
      poll(),
      poll(),
      poll(),
      poll()).map(_.values.head)
    
    val Seq(s0, s1, s2, s3) = results 
    
    assert(s0 == Success(Running))
    assert(s1 == Failure(exception))
    assert(s2 == Success(Done))
    assert(s3 == Failure(exception))
      
    assert(client.params.value == Seq(taskId -> zero, taskId -> zero, taskId -> zero, taskId -> zero))
  }
}
