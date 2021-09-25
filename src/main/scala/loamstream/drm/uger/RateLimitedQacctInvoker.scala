package loamstream.drm.uger

import scala.concurrent.duration.Duration
import scala.util.Try

import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import loamstream.util.Processes
import loamstream.util.RateLimiter
import loamstream.util.RunResults
import loamstream.util.ValueBox
import monix.execution.Scheduler

/**
 * @author clint
 * Sep 8, 2020
 */
final class RateLimitedQacctInvoker private[uger] (
    binaryName: String,
    invokeBinary: String => Try[RunResults],
    maxSize: Int, 
    maxAge: Duration,
    maxRetries: Int,
    scheduler: Scheduler) extends Loggable {

  //TODO
  private implicit val sch = scheduler
  
  val commandInvoker: CommandInvoker.Async.JustOnce[String] = new CommandInvoker.Async.JustOnce[String](
    binaryName, 
    taskArrayId => getLimiter(taskArrayId).apply,
    isSuccess = RunResults.SuccessPredicate.zeroIsSuccess)
  
  import RateLimitedQacctInvoker.JobId
  import RateLimitedQacctInvoker.Limiter
  import RateLimitedQacctInvoker.LimiterMap
  import RateLimitedQacctInvoker.makeTokens
  
  private val limiters: ValueBox[LimiterMap] = ValueBox(Map.empty)
  
  private[uger] def currentLimiterMap: LimiterMap = limiters.value 
  
  private def makeLimiter(taskArrayJobId: JobId): Limiter = {
    RateLimiter.withMaxAge(maxAge) {
      invokeBinary(taskArrayJobId)
    }
  }
  
  private def withLimiterFor(taskArrayJobId: JobId, limiterMap: LimiterMap): (LimiterMap, Limiter) = {
    limiterMap.get(taskArrayJobId) match {
      case Some(limiter) => (limiterMap, limiter)
      case None => {
        val newLimiter = makeLimiter(taskArrayJobId)
    
        val newMap = limiterMap + (taskArrayJobId -> newLimiter)
    
        (newMap, newLimiter)
      }
    }
  }
  
  private def getLimiter(taskArrayJobId: String): Limiter = limiters.getAndUpdate { limiterMap =>
    withLimiterFor(taskArrayJobId, limiterMap)
  }
}

object RateLimitedQacctInvoker extends Loggable {
  private type JobId = String
  private type Limiter = RateLimiter[RunResults]
  private type LimiterMap = Map[JobId, Limiter]
  
  def fromActualBinary(
      binaryName: String,
      maxSize: Int, 
      maxAge: Duration,
      maxRetries: Int, 
      scheduler: Scheduler): RateLimitedQacctInvoker = {
    
    def invokeBinary(taskArrayJobId: JobId): Try[RunResults] = Try {
      val tokens = makeTokens(binaryName, taskArrayJobId)
      
      debug(s"Invoking '$binaryName': '${tokens.mkString(" ")}'")
      
      Processes.runSync(tokens)()
    }
    
    new RateLimitedQacctInvoker(
        binaryName = binaryName,
        invokeBinary = invokeBinary,
        maxSize = maxSize, 
        maxAge = maxAge,
        maxRetries = maxRetries,
        scheduler = scheduler)
  }
  
  private[uger] def makeTokens(actualBinary: String = "qacct", jobNumber: JobId): Seq[String] = {
    Seq(actualBinary, "-j", jobNumber)
  }
}
