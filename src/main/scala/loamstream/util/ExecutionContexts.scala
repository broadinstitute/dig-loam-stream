package loamstream.util

import scala.concurrent.ExecutionContext

/**
 * @author clint
 * @author kaan
 * Nov 23, 2016
 */
object ExecutionContexts {
  /**
   * Creates and returns a fixed-size pool of daemon threads that will shutdown when non-daemon
   * threads complete so JVM is not prevented from exiting.
   * @param numThreads size of thread pool
   * @return Success wrapping the JobStatus corresponding to the code obtained from UGER,
   * or Failure if the job id isn't known.  (Lamely, this can occur if the job is finished.)
   */
  def threadPool(numThreads: Int): (ExecutionContext, Terminable) = {
    val (es, terminable) = {
      ExecutorServices.threadPool(numThreads, ExecutorServices.OnlyDaemonThreads)
    }
    
    (ExecutionContext.fromExecutorService(es), terminable)
  }
  
  def singleThread: (ExecutionContext, Terminable) = threadPool(1)
  
  def oneThreadPerCpu: (ExecutionContext, Terminable) = threadPool(ThisMachine.numCpus)
  
  val defaultThreadPoolSize: Int = 40 //scalastyle:ignore magic.number
  
  def withThreadPool[A](numThreads: Int = defaultThreadPoolSize)(f: ExecutionContext => A): A = {
    val (ec, ecHandle) = threadPool(numThreads)

    try { f(ec) } finally { ecHandle.stop() }
  }
}
