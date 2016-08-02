package loamstream.uger

import org.scalatest.FunSuite

import scala.util.Success
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import monix.execution.Scheduler

import scala.collection.mutable

/**
 * @author clint
 * date: Jul 6, 2016
 */
final class JobsTest extends FunSuite {
  test("monitor() - happy path") {
    import JobStatus._
    
    implicit val scheduler = Scheduler.singleThread("monixScheduler")
    
    val client = MockDrmaaClient(Success(Queued), Success(Running), Success(Done))
    
    val poller = Poller.drmaa(client)
    
    val jobId = "bar"
    
    val statuses = Jobs.monitor(poller, 2)(jobId)
    
    val buf: mutable.Buffer[JobStatus] = new ArrayBuffer
    
    val fut = statuses.foreach(buf += _)

    Await.ready(fut, Duration.Inf)
    
    assert(buf == Seq(Queued, Running, Done))
    
    import scala.concurrent.duration._
    
    val expectedParamTuple = jobId -> 0.5.seconds
    
    assert(client.params == Seq(expectedParamTuple, expectedParamTuple, expectedParamTuple))
  }
}