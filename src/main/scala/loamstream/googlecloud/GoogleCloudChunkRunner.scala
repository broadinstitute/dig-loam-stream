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
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import loamstream.model.jobs.JobState.CommandResult
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.GoogleResources

/**
 * @author clint
 * Nov 28, 2016
 */
final case class GoogleCloudChunkRunner(
    client: DataProcClient, 
    googleConfig: GoogleCloudConfig,
    delegate: ChunkRunner) extends ChunkRunnerFor(ExecutionEnvironment.Google) with Terminable with Loggable {
  
  private[googlecloud] lazy val singleThreadedExecutor: ExecutorService = Executors.newSingleThreadExecutor
  
  private implicit lazy val singleThreadedExecutionContext: ExecutionContext = {
    ExecutionContext.fromExecutorService(singleThreadedExecutor)
  }
  
  //NB: Ensure that we only start the cluster once from this Runner
  private lazy val init: Unit = client.startCluster()
  
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
    
    quietly("Error stopping cluster")(deleteClusterIfNecessary())
  }
  
  private[googlecloud] def deleteClusterIfNecessary(): Unit = {
    //NB: If anything goes wrong determining whether or not the cluster is up, try to shut it down
    //anyway, to be safe.
    Try(client.isClusterRunning) match {
      case Success(true) => client.deleteCluster()
      case Success(false) => info("Cluster not running, not attempting to shut it down")
      case Failure(e) => {
        warn(s"Error determining cluster status, attempting to shut down cluster anyway")
        
        client.deleteCluster()
      }
    }
  }
  
  private[googlecloud] def runJobsSequentially(jobs: Set[LJob]): Observable[Map[LJob, JobState]] = {
    Observables.observeAsync {
      withCluster(client) {
        val singleJobResults = jobs.toSeq.map(runSingle(delegate))
          
        Maps.mergeMaps(singleJobResults)
      }
    }
  }

  private[googlecloud] def runSingle(delegate: ChunkRunner)(job: LJob): Map[LJob, JobState] = {
    //NB: Enforce single-threaded execution, since we don't want multiple jobs running 
    //on the same cluster simultaneously
    import ObservableEnrichments._
    import GoogleCloudChunkRunner.addCluster
    

    val futureResult = delegate.run(Set(job)).map(addCluster(googleConfig.clusterId)).firstAsFuture
    
    Futures.waitFor(futureResult)
  }
  
  private[googlecloud] def withCluster[A](client: DataProcClient)(f: => A): A = {
    init
      
    f
  }
}

object GoogleCloudChunkRunner {
  private[googlecloud] def addCluster(cluster: String)(jobStates: Map[LJob, JobState]): Map[LJob, JobState] = {
    jobStates.map { 
      case (job, state @ CommandResult(exitStatus, Some(localResources: LocalResources))) => {
        val googleResources = GoogleResources.fromClusterAndLocalResources(cluster, localResources)
        
        job -> state.withResources(googleResources)
      }
      case tuple => tuple
    }
  }
}
