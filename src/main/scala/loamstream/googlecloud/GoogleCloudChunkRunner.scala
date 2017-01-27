package loamstream.googlecloud

import scala.concurrent.ExecutionContext

import loamstream.model.execute.ChunkRunner
import loamstream.model.execute.ChunkRunnerFor
import loamstream.model.execute.ExecutionEnvironment
import loamstream.model.jobs.JobState
import loamstream.util.Terminable
import loamstream.util.ExecutorServices
import java.util.concurrent.Executors
import loamstream.util.Loggable
import java.util.concurrent.ExecutorService
import loamstream.util.Maps
import loamstream.model.jobs.LJob
import loamstream.util.Futures
import loamstream.util.Throwables
import loamstream.util.ObservableEnrichments
import rx.lang.scala.Observable
import loamstream.util.Observables

/**
 * @author clint
 * Nov 28, 2016
 */
final case class GoogleCloudChunkRunner(
    client: DataProcClient, 
    delegate: ChunkRunner) extends ChunkRunnerFor(ExecutionEnvironment.Google) with Terminable with Loggable {
  
  private[googlecloud] lazy val singleThreadedExecutor: ExecutorService = Executors.newSingleThreadExecutor
  
  private implicit lazy val singleThreadedExecutionContext: ExecutionContext = {
    ExecutionContext.fromExecutorService(singleThreadedExecutor)
  }
  
  override def maxNumJobs: Int = delegate.maxNumJobs
  
  override def run(jobs: Set[LJob]): Observable[Map[LJob, JobState]] = {
    def emptyResults: Observable[Map[LJob, JobState]] = Observable.just(Map.empty)
  
    if(jobs.isEmpty) { emptyResults }
    else { runJobsSequentially(jobs) }
  }
  
  override def stop(): Unit = {
    import Throwables._
    import scala.concurrent.duration._
    
    quietly("Error shutting down Executor")(ExecutorServices.shutdown(singleThreadedExecutor, 5.seconds))
  }
  
  import GoogleCloudChunkRunner.{withCluster, runSingle}
  
  private[googlecloud] def runJobsSequentially(jobs: Set[LJob]): Observable[Map[LJob, JobState]] = {
    Observables.observeAsync {
      withCluster(client) {
        val singleJobResults = jobs.toSeq.map(runSingle(delegate))
          
        Maps.mergeMaps(singleJobResults)
      }
    }
  }
}

object GoogleCloudChunkRunner {
  //TODO: Make one cluster per set of jobs, instead of keeping one around for multiple chunks, over the lifetime
  //of this whole runner
  //TODO: Avoid this lock :(
  private[this] val lock = new AnyRef
  
  private[googlecloud] def withCluster[A](client: DataProcClient)(f: => A): A = lock.synchronized {
    try {
      client.startCluster()
      
      f
    } finally {
      client.deleteCluster()
    }
  }
  
  private[googlecloud] def runSingle(delegate: ChunkRunner)(job: LJob): Map[LJob, JobState] = {
    //NB: Enforce single-threaded execution, since we don't want multiple jobs running 
    //on the same cluster simultaneously
    import ObservableEnrichments._
    
    val futureResult = delegate.run(Set(job)).firstAsFuture
    
    Futures.waitFor(futureResult)
  }
}
