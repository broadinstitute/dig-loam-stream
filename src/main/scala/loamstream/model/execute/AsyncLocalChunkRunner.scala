package loamstream.model.execute

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import AsyncLocalChunkRunner.defaultMaxNumJobs
import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.model.jobs.commandline.ProcessLoggers
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.Throwables
import rx.lang.scala.Observable

/**
 * @author clint
 * Nov 22, 2016
 */
final case class AsyncLocalChunkRunner(
    executionConfig: ExecutionConfig,
    maxNumJobs: Int = defaultMaxNumJobs)
    (implicit context: ExecutionContext) extends ChunkRunnerFor(EnvironmentType.Local) {

  import AsyncLocalChunkRunner._
  
  override def run(
      jobs: Set[LJob], 
      jobOracle: JobOracle, 
      shouldRestart: LJob => Boolean): Observable[Map[LJob, RunData]] = {
    
    if(jobs.isEmpty) { Observable.just(Map.empty) }
    else {
      import LocalJobStrategy.canBeRun
      
      require(
          jobs.forall(canBeRun), 
          s"Expected only LocalJobs, but found ${jobs.filterNot(canBeRun).mkString(",")}")
      
      def exec(job: LJob): Observable[RunData] = {
        Observable.from(executeSingle(executionConfig, jobOracle, job, shouldRestart))
      }

      val executionObservables: Seq[Observable[RunData]] = jobs.toSeq.map(exec)
        
      val z: Map[LJob, RunData] = Map.empty
      
      //NB: Note the use of scan() here.  It ensures that an item is emitted for a job as soon as that job finishes,
      //instead of only once when all the jobs in a chunk finish.
      Observables.merge(executionObservables).scan(z) { (acc, runData) => acc + (runData.job -> runData) }
    }
  }
}

object AsyncLocalChunkRunner extends Loggable {
  def defaultMaxNumJobs: Int = Runtime.getRuntime.availableProcessors
  
  def executeSingle(
      executionConfig: ExecutionConfig,
      jobOracle: JobOracle,
      job: LJob, 
      shouldRestart: LJob => Boolean)(implicit executor: ExecutionContext): Future[RunData] = {
    
    job.transitionTo(JobStatus.Running)
    
    val jobDir = jobOracle.dirFor(job)
    
    val processLogger = ProcessLoggers.forNamedJob(jobDir)
    
    val result = LocalJobStrategy.execute(job, jobDir, processLogger)

    import loamstream.util.Futures.Implicits._
  
    def closeProcessLogger(ignored: RunData): Unit = {
      Throwables.quietly("Closing process logger failed")(processLogger.close())
    }
    
    result.withSideEffect(closeProcessLogger).withSideEffect(handleResultOfExecution(shouldRestart))
  }
  
  private[execute] def handleResultOfExecution(shouldRestart: LJob => Boolean)(runData: RunData): Unit = {
    debug(s"Handling result of execution: ${runData.job} => $runData")
    
    val newStatus = ExecuterHelpers.determineFinalStatus(shouldRestart, runData.jobStatus, runData.job)
    
    runData.job.transitionTo(newStatus)
  }
}
