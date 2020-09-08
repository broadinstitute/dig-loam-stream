package loamstream.util

import scala.concurrent.ExecutionContext

import loamstream.util.ExecutorServices.Defaults
import loamstream.util.ExecutorServices.QueueStrategy
import loamstream.util.ExecutorServices.RejectedExecutionStrategy


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
  def threadPool(
      numThreads: Int,
      baseName: String = defaultPoolName,
      queueStrategy: QueueStrategy = Defaults.queueStrategy,
      rejectedStrategy: RejectedExecutionStrategy = Defaults.rejectedExecutionStrategy): (ExecutionContext, Terminable) = {
    
    val (es, terminable) = ExecutorServices.threadPool(
      numThreads, 
      baseName,
      ExecutorServices.DaemonFlag.OnlyDaemonThreads, 
      queueStrategy, 
      rejectedStrategy)
    
    (ExecutionContext.fromExecutorService(es), terminable)
  }
  
  def singleThread(
      baseName: String = s"${defaultPoolName}-single",
      queueStrategy: QueueStrategy = Defaults.queueStrategy,
      rejectedStrategy: RejectedExecutionStrategy = Defaults.rejectedExecutionStrategy): (ExecutionContext, Terminable) = {
    
    threadPool(1, baseName, queueStrategy, rejectedStrategy)
  }

  def oneThreadPerCpu(
      baseName: String = s"${defaultPoolName}-onePerCpu",
      queueStrategy: QueueStrategy = Defaults.queueStrategy,
      rejectedStrategy: RejectedExecutionStrategy = Defaults.rejectedExecutionStrategy): (ExecutionContext, Terminable) = {
    
    threadPool(ThisMachine.numCpus, baseName, queueStrategy, rejectedStrategy)
  }

  private def defaultPoolName: String = s"LS-pool-${poolNumbers.next()}"
    
  private val poolNumbers: Sequence[Int] = Sequence()
  
  val defaultThreadPoolSize: Int = 40 //scalastyle:ignore magic.number
  
  def withThreadPool[A](numThreads: Int = defaultThreadPoolSize)(f: ExecutionContext => A): A = {
    val (ec, ecHandle) = threadPool(numThreads)

    try { f(ec) } finally { ecHandle.stop() }
  }
}
