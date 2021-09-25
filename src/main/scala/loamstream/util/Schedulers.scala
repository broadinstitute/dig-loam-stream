package loamstream.util

import monix.execution.Scheduler

/**
 * @author clint
 * Oct 21, 2016
 */
object Schedulers {
  def backedByThreadPool(numThreads: Int): (Scheduler, Terminable) = {
    val (ec, handle) = ExecutionContexts.threadPool(numThreads)
    
    val scheduler: Scheduler = Scheduler(ec)
    
    scheduler -> handle
  }
}
