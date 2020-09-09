package loamstream.drm.uger

import loamstream.drm.AccountingCommandInvoker
import loamstream.drm.DrmTaskId
import rx.lang.scala.Scheduler
import scala.concurrent.ExecutionContext
import loamstream.util.CommandInvoker
import loamstream.util.Processes
import loamstream.util.RateLimitedCache
import scala.util.Try
import loamstream.util.RunResults
import loamstream.util.ValueBox

/**
 * @author clint
 * Apr 23, 2019
 */
object QacctInvoker extends AccountingCommandInvoker.Companion[DrmTaskId] {
  override def makeTokens(actualBinary: String = "qacct", taskId: DrmTaskId): Seq[String] = {
    Seq(actualBinary, "-j", taskId.jobId, "-t", taskId.taskIndex.toString)
  }
  
  object ByTaskArray extends AccountingCommandInvoker.Companion[String] {
    override def useActualBinary(
        maxRetries: Int, 
        binaryName: String,
        scheduler: Scheduler)(implicit ec: ExecutionContext): CommandInvoker.Async[String] = {
      
      def invokeBinaryFor(param: String) = {
        val tokens = makeTokens(binaryName, param)
        
        debug(s"Invoking '$binaryName': '${tokens.mkString(" ")}'")
        
        Processes.runSync(tokens)()
      }
      
      val notRetrying = maxRetries == 0
      
      def makeCache(taskArrayId: String): RateLimitedCache[RunResults] = {
        import scala.concurrent.duration._
        
        new RateLimitedCache(() => invokeBinaryFor(taskArrayId), 5.seconds)
      }
      
      val caches: ValueBox[Map[String, RateLimitedCache[RunResults]]] = ValueBox(Map.empty)
      
      def getCache(tid: String): RateLimitedCache[RunResults] = caches.getAndUpdate { cacheMap =>
        val newCacheMap = {
          if(cacheMap.contains(tid)) { cacheMap } 
          else {
            val cache = makeCache(tid)
            
            cacheMap + (tid -> cache)
          }
        }
        
        (newCacheMap, newCacheMap(tid))
      }
      
      val invokeOnce = new CommandInvoker.Async.JustOnce[String](
          binaryName, 
          tid => getCache(tid).apply())
      
      if(notRetrying) {
        invokeOnce
      } else {
        new CommandInvoker.Async.Retrying[String](delegate = invokeOnce, maxRetries = maxRetries, scheduler = scheduler)
      }
    }
    
    override def makeTokens(actualBinary: String = "qacct", jobNumber: String): Seq[String] = {
      Seq(actualBinary, "-j", jobNumber)
    }
  }
}
