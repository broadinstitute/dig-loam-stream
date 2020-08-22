package loamstream.util

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
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
      daemonFlag: DaemonFlag = ExecutorServices.OnlyDaemonThreads): (ExecutorService, Terminable) = {
    
    val executor = {
      if(daemonFlag.onlyDaemonThreads) { Executors.newFixedThreadPool(numThreads, factoryWithDaemonThreads) }
      else { Executors.newFixedThreadPool(numThreads) }
    }
    
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
  
  /**
   * @return ThreadFactory that produces only daemon threads
   */
  private def factoryWithDaemonThreads: ThreadFactory = new ThreadFactory {
    private val defaultFactory: ThreadFactory = Executors.defaultThreadFactory

    override def newThread(r: Runnable): Thread = {
      val thread = defaultFactory.newThread(r)
      thread.setDaemon(true)
      thread
    }
  }
  
  sealed trait DaemonFlag {
    def noDaemonThreads: Boolean = this == NoDaemonThreads
    def onlyDaemonThreads: Boolean = this == OnlyDaemonThreads
  }
  
  case object NoDaemonThreads extends DaemonFlag
  case object OnlyDaemonThreads extends DaemonFlag
}
