package loamstream.drm.uger

import org.scalatest.FunSuite
import scala.util.Try
import loamstream.util.RunResults
import scala.util.Success
import scala.util.Failure
import scala.concurrent.duration._
import rx.lang.scala.schedulers.IOScheduler
import loamstream.TestHelpers.waitFor
import loamstream.util.Sequence

/**
 * @author clint
 * Sep 10, 2020
 */
final class RateLimitedCachedQacctInvokerTest extends FunSuite {
  test("makeTokens") {
    import RateLimitedCachedQacctInvoker.makeTokens
    
    assert(makeTokens(jobNumber = "12345") === Seq("qacct", "-j", "12345"))
    
    assert(makeTokens(actualBinary = "lalala", jobNumber = "12345") === Seq("lalala", "-j", "12345"))
  }
  
  test("Happy path") {
    val taskArrayId0 = "0"
    val taskArrayId1 = "1"
    val taskArrayId2 = "2"
    val taskArrayId3 = "3"

    val sequences: Map[String, Sequence[Int]] = Map(
        taskArrayId0 -> Sequence(),
        taskArrayId1 -> Sequence(),
        taskArrayId2 -> Sequence(),
        taskArrayId3 -> Sequence())
    
    def successfulInvocationFn: String => Try[RunResults] = { taskArrayJobId =>
      val seq = sequences(taskArrayJobId)
      
      Success(RunResults("MOCK", 0, Seq(s"$taskArrayJobId-${seq.next()}"), Nil))
    }
    
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val caches = new RateLimitedCachedQacctInvoker(
        "MOCK", 
        successfulInvocationFn, 
        maxSize = 3, 
        maxAge = 1.second, 
        maxRetries = 0,
        scheduler = IOScheduler())
    
    val invoker = caches.commandInvoker
    
    assert(waitFor(invoker.apply(taskArrayId0)).stdout.head === "0-0")
    assert(waitFor(invoker.apply(taskArrayId0)).stdout.head === "0-0")
    
    Thread.sleep(1000)
    
    assert(waitFor(invoker.apply(taskArrayId0)).stdout.head === "0-1")
    
    assert(waitFor(invoker.apply(taskArrayId1)).stdout.head === "1-0")
    assert(waitFor(invoker.apply(taskArrayId2)).stdout.head === "2-0")
    
    assert(waitFor(invoker.apply(taskArrayId1)).stdout.head === "1-0")
    assert(waitFor(invoker.apply(taskArrayId2)).stdout.head === "2-0")
    
    Thread.sleep(1000)
    
    assert(waitFor(invoker.apply(taskArrayId0)).stdout.head === "0-2")
    
    assert(waitFor(invoker.apply(taskArrayId1)).stdout.head === "1-1")
    assert(waitFor(invoker.apply(taskArrayId2)).stdout.head === "2-1")
    
    assert(caches.currentCacheMap.keySet === Set(taskArrayId0, taskArrayId1, taskArrayId2))
    
    assert(waitFor(invoker.apply(taskArrayId3)).stdout.head === "3-0")
    
    //Oldest cached value should be evicted
    assert(caches.currentCacheMap.keySet === Set(taskArrayId1, taskArrayId2, taskArrayId3))
  }
}
