package loamstream.drm.uger

import scala.concurrent.duration.Duration
import loamstream.util.RateLimitedCache
import loamstream.util.RunResults
import loamstream.util.ValueBox
import loamstream.util.Processes
import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import rx.lang.scala.Scheduler
import scala.concurrent.ExecutionContext

/**
 * @author clint
 * Sep 8, 2020
 */
final class RateLimitedQacctCache(
    maxSize: Int, 
    maxAge: Duration,
    maxRetries: Int, 
    binaryName: String,
    scheduler: Scheduler)(implicit ec: ExecutionContext) extends Loggable {
  
  private def makeTokens(actualBinary: String = "qacct", jobNumber: String): Seq[String] = {
    Seq(actualBinary, "-j", jobNumber)
  }
  
  private def invokeBinaryFor(param: String) = {
    val tokens = makeTokens(binaryName, param)
    
    debug(s"Invoking '$binaryName': '${tokens.mkString(" ")}'")
    
    Processes.runSync(tokens)()
  }
  
  private val caches: ValueBox[Map[String, RateLimitedCache[RunResults]]] = ValueBox(Map.empty)
  
  private def makeCache(taskArrayId: String): RateLimitedCache[RunResults] = {
    import scala.concurrent.duration._
        
    new RateLimitedCache(() => invokeBinaryFor(taskArrayId), 5.seconds)
  }
  
  private def addNewCacheEntryFor(taskArrayId: String): Map[String, RateLimitedCache[RunResults]] = {
    val (result, _) = caches.mutateAndGet { cacheMap =>
      if(cacheMap.contains(taskArrayId)) { cacheMap }
      else {
        val mapToAddTo = {
          if(cacheMap.size == maxSize) {
            val (lruTid, _) = cacheMap.minBy { case (_, cache) => cache.lastAccessed }
        
            cacheMap - lruTid
          } else {
            cacheMap
          }
        }
        
        mapToAddTo + (taskArrayId -> makeCache(taskArrayId))
      }
    }
    
    result
  }
  
  private def getCache(tid: String): RateLimitedCache[RunResults] = caches.getAndUpdate { cacheMap =>
    val newCacheMap = {
      if(cacheMap.contains(tid)) { cacheMap } 
      else { addNewCacheEntryFor(tid) }
    }
    
    (newCacheMap, newCacheMap(tid))
  }
      
  val commandInvoker: CommandInvoker.Async.JustOnce[String] = new CommandInvoker.Async.JustOnce[String](
    binaryName, 
    taskArrayId => getCache(taskArrayId).apply())
}
