package loamstream.uger

import org.scalatest.FunSuite

import scala.util.Success
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.Await

import scala.collection.mutable
import loamstream.util.ObservableEnrichments

/**
 * @author clint
 * date: Jul 6, 2016
 */
final class JobsTest extends FunSuite {
  test("monitor() - happy path") {
    import JobStatus._
    
    val client = MockDrmaaClient(Success(Queued), Success(Running), Success(Done))
    
    import scala.concurrent.ExecutionContext.Implicits.global
    import ObservableEnrichments._
    
    val poller = Poller.drmaa(client)
    
    val jobId = "bar"
    
    val statuses = Jobs.monitor(poller, 2)(jobId)
    
    val fut = statuses.to[Seq].firstAsFuture

    assert(Await.result(fut, Duration.Inf) == Seq(Queued, Running, Done))
    
    import scala.concurrent.duration._
    
    val expectedParamTuple = jobId -> 0.5.seconds
    
    assert(client.params.value == Seq(expectedParamTuple, expectedParamTuple, expectedParamTuple))
  }
}