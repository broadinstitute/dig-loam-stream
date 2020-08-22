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
  def backedByThreadPool(numThreads: Int): (Scheduler, Terminable) = {
    val (executor, handle) = ExecutorServices.threadPool(numThreads)
    
    val executionContext = ExecutionContext.fromExecutorService(executor)
    
    val scheduler: Scheduler = ExecutionContextScheduler(executionContext)
    
    scheduler -> handle
  }
}
