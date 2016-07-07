package loamstream.uger

import org.scalatest.FunSuite
import scala.util.Try
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Success
import java.nio.file.Path
import scala.concurrent.Await
import scala.util.Failure

/**
 * @author clint
 * date: Jul 6, 2016
 */
final class PollerTest extends FunSuite {
  import JobStatus._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._
  
  test("drama().poll() - happy path") {
    val client = MockDrmaaClient(Success(Running), Success(Running), Success(Done))
    
    val poller = Poller.drmaa(client)
    
    val jobId = "foo"
    
    val future = for {
      s0 <- poller.poll(jobId, 1.second)
      s1 <- poller.poll(jobId, 2.seconds)
      s2 <- poller.poll(jobId, 3.seconds)
    } yield {
      assert(s0 == Success(Running))
      assert(s1 == Success(Running))
      assert(s2 == Success(Done))
      
      assert(client.params == Seq(jobId -> 1.second, jobId -> 2.seconds, jobId -> 3.seconds))
      
      ()
    }
    
    Await.ready(future, Duration.Inf)
  }
  
  test("drama().poll() - some failures") {
    val exception = new Exception with scala.util.control.NoStackTrace
    
    val client = MockDrmaaClient(Success(Running), Failure(exception), Success(Done), Failure(exception))
    
    val poller = Poller.drmaa(client)
    
    val jobId = "foo"
    
    val future = for {
      s0 <- poller.poll(jobId, 1.second)
      s1 <- poller.poll(jobId, 2.seconds)
      s2 <- poller.poll(jobId, 3.seconds)
      s3 <- poller.poll(jobId, 4.seconds)
    } yield {
      assert(s0 == Success(Running))
      assert(s1 == Failure(exception))
      assert(s2 == Success(Done))
      assert(s3 == Failure(exception))
      
      assert(client.params == Seq(jobId -> 1.second, jobId -> 2.seconds, jobId -> 3.seconds, jobId -> 4.seconds))
      
      ()
    }
    
    Await.ready(future, Duration.Inf)
  }
}
