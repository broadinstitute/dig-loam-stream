package loamstream.util

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt


/**
 * @author clint
 * Oct 21, 2016
 */
object ExecutorServices {
  def threadPool(
      numThreads: Int, 
      baseName: String,
      daemonFlag: DaemonFlag = ExecutorServices.DaemonFlag.OnlyDaemonThreads,
      queueStrategy: QueueStrategy = Defaults.queueStrategy,
      rejectedStrategy: RejectedExecutionStrategy = Defaults.rejectedStrategy): (ExecutorService, Terminable) = {
    
    val threadFactory = {
      if(daemonFlag.onlyDaemonThreads) { ThreadFactories.factoryWithDaemonThreads(baseName) }
      else { ThreadFactories.default }
    }
     
    val executor = new ThreadPoolExecutor(
      numThreads, 
      numThreads, 
      0L, 
      TimeUnit.MILLISECONDS, 
      queueStrategy.toBlockingQueue, 
      threadFactory, 
      rejectedStrategy.toRejectedExecutionHandler)
    
    (executor, Terminable { shutdown(executor) }) 
  }
  
  def shutdown(es: ExecutorService, howLongToWaitForWorkToFinish: Duration = 5.seconds): Unit = {
    try {
      es.shutdown()
      
      es.awaitTermination(howLongToWaitForWorkToFinish.toMillis, TimeUnit.MILLISECONDS)
    } finally {
      es.shutdownNow()
    }
  }
  
  private[ExecutorServices] object ThreadFactories {
    def default: ThreadFactory = Executors.defaultThreadFactory
  
    /**
     * @return ThreadFactory that produces only daemon threads
     */
    def factoryWithDaemonThreads(baseName: String): ThreadFactory = new ThreadFactory {
      private val defaultFactory: ThreadFactory = default

      private val threadNumbers: Sequence[Int] = Sequence()
      
      override def newThread(r: Runnable): Thread = {
        val thread = defaultFactory.newThread(r)
        thread.setName(s"${baseName}-${threadNumbers.next()}")
        thread.setDaemon(true)
        thread
      }
    }
  }
  
  sealed trait DaemonFlag {
    def noDaemonThreads: Boolean = this == DaemonFlag.NoDaemonThreads
    def onlyDaemonThreads: Boolean = this == DaemonFlag.OnlyDaemonThreads
  }
  
  object DaemonFlag {
    case object NoDaemonThreads extends DaemonFlag
    case object OnlyDaemonThreads extends DaemonFlag
  }
  
  sealed trait QueueStrategy {
    def toBlockingQueue: BlockingQueue[Runnable]
  }
  
  object QueueStrategy {
    case object Unbounded extends QueueStrategy {
      override def toBlockingQueue: BlockingQueue[Runnable] = new LinkedBlockingQueue
    }
    
    final case class Bounded(maxSize: Int) extends QueueStrategy {
      override def toBlockingQueue: BlockingQueue[Runnable] = new ArrayBlockingQueue(maxSize)
    }
  }
  
  sealed trait RejectedExecutionStrategy {
    def toRejectedExecutionHandler: RejectedExecutionHandler
  }
  
  object RejectedExecutionStrategy {
    case object Abort extends RejectedExecutionStrategy {
      def toRejectedExecutionHandler: RejectedExecutionHandler = new ThreadPoolExecutor.AbortPolicy
    }
    
    case object Drop extends RejectedExecutionStrategy {
      def toRejectedExecutionHandler: RejectedExecutionHandler = new ThreadPoolExecutor.DiscardPolicy
    }
  }
  
  object Defaults {
    def queueStrategy: QueueStrategy = QueueStrategy.Unbounded
    
    def rejectedStrategy: RejectedExecutionStrategy = RejectedExecutionStrategy.Abort
  }
}
