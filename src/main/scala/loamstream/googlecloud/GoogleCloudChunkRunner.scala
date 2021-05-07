package loamstream.googlecloud

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import GoogleCloudChunkRunner.ClusterStatus
import loamstream.model.execute.ChunkRunner
import loamstream.model.execute.ChunkRunnerFor
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.GoogleSettings
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.util.ExecutorServices
import loamstream.util.Loggable
import loamstream.util.Terminable
import loamstream.util.ValueBox
import monix.reactive.Observable
import monix.execution.Scheduler


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
  
  private lazy val singleThreadedScheduler: Scheduler = Scheduler(singleThreadedExecutionContext)
  
  private val currentClusterConfig: ValueBox[Option[ClusterConfig]] = ValueBox(None)
  
  override def run(
      jobs: Set[LJob], 
      jobOracle: JobOracle): Observable[(LJob, RunData)] = {
    
    if(jobs.isEmpty) { Observable.empty }
    else { runJobsSequentially(jobs, jobOracle) }
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
  
  private[googlecloud] def startClusterIfNecessary(clusterConfig: ClusterConfig): Unit = {
    def start(): Unit = {
      client.startCluster(clusterConfig)
      
      currentClusterConfig := Option(clusterConfig) 
    }
    
    //NB: If anything goes wrong determining whether or not the cluster is up, try to shut it down
    //anyway, to be safe.
    determineClusterStatus() match {
      case ClusterStatus.Running => debug("Cluster already running, not attempting to start it")  
      case ClusterStatus.NotRunning => start()
      case ClusterStatus.Undetermined(e) => {
        warn(s"Error determining cluster status, attempting to start cluster anyway", e)
        
        start()
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
      jobOracle: JobOracle): Observable[(LJob, RunData)] = {
    
    def doRunSingle(j: LJob): (LJob, RunData) = {
      def runDataForNonGoogleJob = RunData(
          job = j, 
          settings = j.initialSettings, 
          jobStatus = JobStatus.CouldNotStart, 
          jobResult = None, 
          terminationReasonOpt = None)
      
      j.initialSettings match {
        case GoogleSettings(_, clusterConfig) => withCluster(clusterConfig) {
          runSingle(delegate, jobOracle)(j)
        }
        case settings => j -> runDataForNonGoogleJob
      }
    }
    
    Observable.from(jobs).subscribeOn(singleThreadedScheduler).map(doRunSingle)
  }

  private[googlecloud] def runSingle(
      delegate: ChunkRunner, 
      jobOracle: JobOracle)(job: LJob): (LJob, RunData) = {
    
    //NB: Enforce single-threaded execution, since we don't want multiple jobs running 
    import GoogleCloudChunkRunner.addCluster
    //on the same cluster simultaneously
    import loamstream.util.Observables.Implicits._

    val googleSettings = job.initialSettings match {
      case gs: GoogleSettings => gs
      case _ => sys.error(s"Only jobs with Google settings are supported, but got ${job.initialSettings}")
    }
    
    val futureResult = {
      implicit val sch = singleThreadedScheduler
      
      delegate.run(Set(job), jobOracle).map(addCluster(googleSettings.clusterId)).lastAsFuture
    }
    
    //TODO: add some timeout
    Await.result(futureResult, Duration.Inf)
  }
  
  private[googlecloud] def withCluster[A](clusterConfig: ClusterConfig)(f: => A): A = {
    def differentCurrentClusterDefined(currentClusterConfigOpt: Option[ClusterConfig]): Boolean = {
      currentClusterConfigOpt.isDefined && (currentClusterConfigOpt != Option(clusterConfig))
    }
    
    currentClusterConfig.get { currentClusterConfigOpt =>
      if(differentCurrentClusterDefined(currentClusterConfigOpt)) {
        deleteClusterIfNecessary()
      }
    
      startClusterIfNecessary(clusterConfig)
      
      f
    }
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
                                     (jobAndExecution: (LJob, RunData)): (LJob, RunData) = {
    jobAndExecution match {
      case (job, runData @ RunData.WithLocalResources(localResources: LocalResources)) => {
        val googleResources = GoogleResources.fromClusterAndLocalResources(cluster, localResources)

        job -> runData.withResources(googleResources)
      }
      case tuple => tuple
    }
  }
}
