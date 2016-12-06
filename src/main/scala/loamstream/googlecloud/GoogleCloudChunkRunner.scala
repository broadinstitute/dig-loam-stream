package loamstream.googlecloud

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.execute.AsyncLocalChunkRunner
import loamstream.model.execute.ChunkRunner
import loamstream.model.execute.ChunkRunnerFor
import loamstream.model.execute.ExecutionEnvironment
import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob
import loamstream.util.Terminable
import loamstream.util.ExecutorServices
import java.util.concurrent.Executors
import loamstream.util.Loggable
import scala.util.control.NonFatal
import java.util.concurrent.ExecutorService

/**
 * @author clint
 * Nov 28, 2016
 */
final case class GoogleCloudChunkRunner(
    client: DataProcClient, 
    delegate: ChunkRunner = AsyncLocalChunkRunner(1)) extends 
  ChunkRunnerFor(ExecutionEnvironment.Google) with Terminable with Loggable {
  
  private lazy val singleThreadedExecutor: ExecutorService = Executors.newSingleThreadExecutor
  
  private lazy val singleThreadedExecutionContext: ExecutionContext = {
    ExecutionContext.fromExecutorService(singleThreadedExecutor)
  }
  
  override def maxNumJobs: Int = delegate.maxNumJobs
  
  override def stop(): Unit = {
    def quietly(f: => Any): Unit = {
      try { f }
      catch { case NonFatal(e) => error("Error shutting down: ", e) }
    }
    
    import scala.concurrent.duration._
    
    quietly(client.deleteClusterIfRunning())
    quietly(ExecutorServices.shutdown(singleThreadedExecutor, 5.seconds))
  }
  
  override def run(jobs: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, JobState]] = {
    if(jobs.nonEmpty) {
      client.doWithCluster {
        //NB: Enforce single-threaded execution, since we don't want multiple jobs running 
        //on the same cluster simultaneously
        //TODO: Make a new cluster for every job?
        delegate.run(jobs)(singleThreadedExecutionContext)
      }
    } else {
      Future.successful(Map.empty)
    }
  }
  
}