package loamstream.googlecloud

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext
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
import monix.execution.Scheduler
import monix.reactive.Observable

import scala.collection.compat._
import monix.eval.Task
import loamstream.util.Observables
import scala.concurrent.Await
import scala.concurrent.duration.Duration


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
      jobs: Iterable[LJob], 
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
  
  private[googlecloud] def deleteClusterIfNecessary(): Unit = currentClusterConfig.foreach { _ =>
    //NB: If anything goes wrong determining whether or not the cluster is up, try to shut it down
    //anyway, to be safe.
    determineClusterStatus() match {
      case ClusterStatus.NotRunning => debug("Cluster not running, not attempting to shut it down")  
      case ClusterStatus.Running => client.stopCluster()
      case ClusterStatus.Undetermined(e) => {
        warn(s"Error determining cluster status, attempting to shut down cluster anyway", e)
        
        client.stopCluster()
      }
    }
  }
  
  private[googlecloud] def startClusterIfNecessary(clusterConfig: ClusterConfig): Unit = currentClusterConfig.foreach { _ =>
    def start(): Unit = {
      client.startCluster(clusterConfig)
      
      currentClusterConfig := Option(clusterConfig) 
    }
    
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
      jobs: Iterable[LJob], 
      jobOracle: JobOracle): Observable[(LJob, RunData)] = {
    
    def doRunSingle(j: LJob): Observable[(LJob, RunData)] = {
      def runDataForNonGoogleJob = RunData(
          job = j, 
          settings = j.initialSettings, 
          jobStatus = JobStatus.CouldNotStart, 
          jobResult = None, 
          terminationReasonOpt = None)
      
      /* j.initialSettings match {
        case GoogleSettings(_, clusterConfig) => withCluster(clusterConfig) {
          runSingle(delegate, jobOracle)(j)
        }
        case settings => Observable(j -> runDataForNonGoogleJob)
      } */

      j.initialSettings match {
        case GoogleSettings(_, clusterConfig) => runSingle(delegate, jobOracle)(j)
        case settings => Observable(j -> runDataForNonGoogleJob)
      }
    }
    
    //NB: Enforce single-threaded execution, since we don't want multiple jobs running 
    //on the same cluster simultaneously
    //NB: .share() is required, or else jobs will run multiple times :\
    //NB: Monix flatMap is like Rx concatMap, so that ordering and at-most-once semantics are preserved
    Observable(jobs.to(Seq): _*).share(singleThreadedScheduler).executeOn(singleThreadedScheduler).flatMap(doRunSingle)
  }

  private[googlecloud] def runSingle(
      delegate: ChunkRunner, 
      jobOracle: JobOracle)(job: LJob): Observable[(LJob, RunData)] = {
    
    val googleSettings @ GoogleSettings(clusterId, clusterConfig) = job.initialSettings match {
      case gs: GoogleSettings => gs
      case _ => sys.error(s"Only jobs with Google settings are supported, but got ${job.initialSettings}")
    }

    def runSynchronously(): (LJob, RunData) = {
      import Observables.Implicits._
      import Scheduler.Implicits.global
      import GoogleCloudChunkRunner.addCluster
      
      Await.result(delegate.run(Set(job), jobOracle).map(addCluster(clusterId)).firstAsFuture, Duration.Inf)
    }

    //NB: Run each job synchronously to guarantee that we only ever have one job and one cluster running at a time.
    //There's a good argument for not having this restriction, but the current behavior is intentional and driven by
    //requests from users.
    Observable.evalOnce {
      withCluster(clusterConfig) {
        runSynchronously()
      }
    //NB: share() and executeOn() are needed to make sure we only ever use the single-threaded Scheduler, helping to 
    //guarantee that only one job and cluster are running at any time.
    }.share(singleThreadedScheduler).executeOn(singleThreadedScheduler)
  }

  private[googlecloud] def withCluster[A](clusterConfig: ClusterConfig)(f: => A): A = { 
    def differentCurrentClusterDefined(currentClusterConfigOpt: Option[ClusterConfig]): Boolean = {
      currentClusterConfigOpt match {
        case Some(cc) => cc != clusterConfig
        case None => false
      }
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
