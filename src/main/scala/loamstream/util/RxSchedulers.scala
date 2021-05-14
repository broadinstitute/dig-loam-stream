package loamstream.util

import scala.concurrent.ExecutionContext
import monix.execution.Scheduler

/**
 * @author clint
 * Oct 21, 2016
 */
object RxSchedulers {
  def backedByThreadPool(numThreads: Int): (Scheduler, Terminable) = {
    val (ec, handle) = ExecutionContexts.threadPool(numThreads)
    
    val scheduler: Scheduler = Scheduler(ec)
    
    scheduler -> handle
  }
}
