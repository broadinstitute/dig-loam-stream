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
import loamstream.util.Maps
import loamstream.model.jobs.LJob
import loamstream.util.Futures
import loamstream.util.Throwables

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
    import Throwables._
    import scala.concurrent.duration._
    
    //quietly("Error shutting down DataProc Cluster")(client.deleteClusterIfRunning())
    quietly("Error shutting down Executor")(ExecutorServices.shutdown(singleThreadedExecutor, 5.seconds))
  }
  
  //TODO: Make one cluster per set of jobs, instead of keeping one around for multiple chunks, over the lifetime
  //of this whole runner
  
  private def withCluster[A](client: DataProcClient)(f: => A): A = {
    try {
      client.startCluster()
      
      f
    } finally {
      client.deleteCluster()
    }
  }
  
  private def runSingle(job: LJob): Map[LJob, JobState] = {
    //NB: Enforce single-threaded execution, since we don't want multiple jobs running 
    //on the same cluster simultaneously
    Futures.waitFor(delegate.run(Set(job))(singleThreadedExecutionContext))
  }
  
  override def run(jobs: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, JobState]] = {
    if(jobs.isEmpty) {
      Future.successful(Map.empty)
    } else {
      Future {
        withCluster(client) {
          val singleJobResults = jobs.toSeq.map(runSingle)
          
          Maps.mergeMaps(singleJobResults)
        }
      }
    }
  }
  
}