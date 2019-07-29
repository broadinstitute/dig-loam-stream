package loamstream.googlecloud

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import loamstream.model.execute.ChunkRunner
import loamstream.model.execute.ChunkRunnerFor
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.util.ExecutorServices
import loamstream.util.Loggable
import loamstream.util.Maps
import loamstream.util.Observables
import loamstream.util.Terminable
import rx.lang.scala.Observable
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.Scheduler
import loamstream.model.jobs.JobOracle
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.GoogleSettings
import GoogleCloudChunkRunner.ClusterStatus


/**
 * @author clint
 * Nov 28, 2016
 */
final case class GoogleCloudChunkRunner(
    client: DataProcClient, 
    googleConfig: GoogleCloudConfig,
    delegate: ChunkRunner) extends ChunkRunnerFor(EnvironmentType.Google) with Terminable with Loggable {
  
  private[googlecloud] lazy val singleThreadedExecutor: ExecutorService = Executors.newSingleThreadExecutor
  
  private implicit lazy val singleThreadedExecutionContext: ExecutionContext = {
    ExecutionContext.fromExecutorService(singleThreadedExecutor)
  }
  
  private lazy val singleThreadedScheduler: Scheduler = ExecutionContextScheduler(singleThreadedExecutionContext)
  
  override def maxNumJobs: Int = delegate.maxNumJobs
  
  override def run(
      jobs: Set[LJob], 
      jobOracle: JobOracle, 
      shouldRestart: LJob => Boolean): Observable[Map[LJob, RunData]] = {
    
    def emptyResults: Observable[Map[LJob, RunData]] = Observable.just(Map.empty)
  
    if(jobs.isEmpty) { emptyResults }
    else { runJobsSequentially(jobs, jobOracle, shouldRestart) }
  }
  
  override def stop(): Unit = {
    import loamstream.util.Throwables._
    import scala.concurrent.duration._
    
    quietly("Error shutting down Executor")(ExecutorServices.shutdown(singleThreadedExecutor, 5.seconds))
    
    //Fail loudly here, since this could cost money
    deleteClusterIfNecessary()
  }
  
  private[googlecloud] def deleteClusterIfNecessary(): Unit = {
    //NB: If anything goes wrong determining whether or not the cluster is up, try to shut it down
    //anyway, to be safe.
    determineClusterStatus() match {
      case ClusterStatus.Running => client.stopCluster()
      case ClusterStatus.NotRunning => debug("Cluster not running, not attempting to shut it down")
      case ClusterStatus.Undetermined(e) => {
        warn(s"Error determining cluster status, attempting to shut down cluster anyway", e)
        
        client.stopCluster()
      }
    }
  }
  
  private[googlecloud] def startClusterIfNecessary(): Unit = {
    //NB: If anything goes wrong determining whether or not the cluster is up, try to shut it down
    //anyway, to be safe.
    determineClusterStatus() match {
      case ClusterStatus.NotRunning => client.startCluster()
      case ClusterStatus.Running => debug("Cluster already running, not attempting to start it")
      case ClusterStatus.Undetermined(e) => {
        warn(s"Error determining cluster status, attempting to start cluster anyway", e)
        
        client.startCluster()
      }
    }
  }
  
  private def determineClusterStatus(): ClusterStatus = {
    Try(client.isClusterRunning) match {
      case Success(true) => ClusterStatus.Running
      case Success(false) => ClusterStatus.NotRunning
      case Failure(e) => ClusterStatus.Undetermined(e)
    }
  }
  
  private[googlecloud] def runJobsSequentially(
      jobs: Set[LJob], 
      jobOracle: JobOracle, 
      shouldRestart: LJob => Boolean): Observable[Map[LJob, RunData]] = {
    
    def doRunSingle(j: LJob): Map[LJob, RunData] = {
      withCluster(client) {
        runSingle(delegate, jobOracle, shouldRestart)(j)
      }
    }
    
    Observable.from(jobs).subscribeOn(singleThreadedScheduler).map(doRunSingle)
  }

  private[googlecloud] def runSingle(
      delegate: ChunkRunner, 
      jobOracle: JobOracle, 
      shouldRestart: LJob => Boolean)(job: LJob): Map[LJob, RunData] = {
    
    //NB: Enforce single-threaded execution, since we don't want multiple jobs running 
    import GoogleCloudChunkRunner.addCluster
    //on the same cluster simultaneously
    import loamstream.util.Observables.Implicits._

    val futureResult = {
      delegate.run(Set(job), jobOracle, shouldRestart).map(addCluster(googleConfig.clusterId)).lastAsFuture
    }
    
    //TODO: add some timeout
    Await.result(futureResult, Duration.Inf)
  }
  
  private[googlecloud] def withCluster[A](client: DataProcClient)(f: => A): A = {
    startClusterIfNecessary()
      
    f
  }
}

object GoogleCloudChunkRunner extends Loggable {
  sealed trait ClusterStatus
  
  object ClusterStatus {
    final case object Running extends ClusterStatus
    final case object NotRunning extends ClusterStatus
    final case class Undetermined(cause: Throwable) extends ClusterStatus
  }
  
  private[googlecloud] def addCluster(cluster: String)
                                     (jobsAndExecutions: Map[LJob, RunData]): Map[LJob, RunData] = {
    jobsAndExecutions.map {
      case (job, runData @ RunData.WithLocalResources(localResources: LocalResources)) => {
        val googleResources = GoogleResources.fromClusterAndLocalResources(cluster, localResources)
        
        //Make sure we've got Google settings
        //TODO: Why weren't the settings GoogleSettings to begin with?
        val newSettings = runData.settings match {
          case LocalSettings => {
            val googleSettings = GoogleSettings(cluster)
            
            debug(s"Munging LocalSettings to ${googleSettings}, grumble grumble")
            
            googleSettings
          }
          case settings => settings
        }
        
        job -> runData.withResources(googleResources).withSettings(newSettings)
      }
      case tuple => tuple
    }
  }
}
