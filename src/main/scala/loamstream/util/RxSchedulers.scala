package loamstream.util

import rx.lang.scala.Scheduler
import rx.schedulers.Schedulers
import scala.concurrent.ExecutionContext
import rx.lang.scala.schedulers.ExecutionContextScheduler

/**
 * @author clint
 * Oct 21, 2016
 */
object RxSchedulers {
  trait SchedulerHandle {
    def shutdown(): Unit
  }
  
  object SchedulerHandle {
    def apply(doShutdown: => Unit): SchedulerHandle = new SchedulerHandle {
      override def shutdown(): Unit = doShutdown
    }
  }
  
  def backedByThreadPool(numThreads: Int): (Scheduler, SchedulerHandle) = {
    val executor = ExecutorServices.threadPool(numThreads)
    
    val executionContext = ExecutionContext.fromExecutorService(executor)
    
    val scheduler: Scheduler = ExecutionContextScheduler(executionContext)
    
    val handle = SchedulerHandle { ExecutorServices.shutdown(executor) }
    
    scheduler -> handle
  }
}