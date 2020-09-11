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
    val (ec, handle) = ExecutionContexts.threadPool(numThreads)
    
    val scheduler: Scheduler = ExecutionContextScheduler(ec)
    
    scheduler -> handle
  }
}
