package loamstream.apps

import rx.lang.scala.Scheduler
import loamstream.util.RxSchedulers

/**
 * @author clint
 * Oct 21, 2016
 */
trait SchedulerHelpers {
  def withThreadPoolScheduler[A](numThreads: Int)(f: Scheduler => A): A = {
    val (scheduler, handle) = RxSchedulers.backedByThreadPool(numThreads)
    
    try { f(scheduler) }
    finally { handle.shutdown() }
  }
}