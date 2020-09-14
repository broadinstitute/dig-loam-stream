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
import scala.util.Try

/**
 * @author clint
 * Sep 8, 2020
 */
final class RateLimitedCachedQacctInvoker private[uger] (
    binaryName: String,
    invokeBinary: String => Try[RunResults],
    maxSize: Int, 
    maxAge: Duration,
    maxRetries: Int,
    scheduler: Scheduler)(implicit ec: ExecutionContext) extends Loggable {

  val commandInvoker: CommandInvoker.Async.JustOnce[String] = new CommandInvoker.Async.JustOnce[String](
    binaryName, 
    taskArrayId => getCache(taskArrayId).apply())
  
  import RateLimitedCachedQacctInvoker.JobId
  import RateLimitedCachedQacctInvoker.Cache
  import RateLimitedCachedQacctInvoker.CacheMap
  import RateLimitedCachedQacctInvoker.makeTokens
  
  private val caches: ValueBox[CacheMap] = ValueBox(Map.empty)
  
  private[uger] def currentCacheMap: CacheMap = caches.value 
  
  private def makeCache(taskArrayJobId: JobId): Cache = {
    RateLimitedCache.withMaxAge(maxAge) {
      invokeBinary(taskArrayJobId)
    }
  }
  
  private def addNewCacheEntryFor(taskArrayJobId: JobId): CacheMap = {
    val (result, _) = caches.mutateAndGet { cacheMap =>
      if(cacheMap.contains(taskArrayJobId)) { cacheMap }
      else {
        val mapToAddTo = {
          if(cacheMap.size == maxSize) {
            val (lruTid, _) = cacheMap.minBy { case (_, cache) => cache.lastAccessed }
        
            cacheMap - lruTid
          } else {
            cacheMap
          }
        }
        
        val newMapping = (taskArrayJobId -> makeCache(taskArrayJobId))
        
        mapToAddTo + newMapping
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
}

object RateLimitedCachedQacctInvoker extends Loggable {
  private type JobId = String
  private type Cache = RateLimitedCache[RunResults]
  private type CacheMap = Map[JobId, Cache]
  
  def fromActualBinary(
      binaryName: String,
      maxSize: Int, 
      maxAge: Duration,
      maxRetries: Int, 
      scheduler: Scheduler)(implicit ec: ExecutionContext): RateLimitedCachedQacctInvoker = {
    
    def invokeBinary(taskArrayJobId: JobId): Try[RunResults] = {
      val tokens = makeTokens(binaryName, taskArrayJobId)
      
      debug(s"Invoking '$binaryName': '${tokens.mkString(" ")}'")
      
      Processes.runSync(tokens)()
    }
    
    new RateLimitedCachedQacctInvoker(
        binaryName = binaryName,
        invokeBinary = invokeBinary,
        maxSize = maxSize, 
        maxAge = maxAge,
        maxRetries = maxRetries,
        scheduler = scheduler)(ec)
  }
  
  private[uger] def makeTokens(actualBinary: String = "qacct", jobNumber: JobId): Seq[String] = {
    Seq(actualBinary, "-j", jobNumber)
  }
}
