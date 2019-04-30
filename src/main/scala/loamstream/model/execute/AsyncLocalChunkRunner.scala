package loamstream.model.execute

import scala.concurrent.ExecutionContext
import loamstream.model.jobs.{Execution, LJob}
import loamstream.util.Maps
import AsyncLocalChunkRunner.defaultMaxNumJobs
import rx.lang.scala.Observable
import loamstream.util.Observables
import scala.concurrent.Future
import loamstream.model.jobs.JobStatus
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.model.jobs.LocalJob
import loamstream.model.jobs.commandline.ProcessLoggers
import loamstream.util.Throwables
import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.RunData
import loamstream.util.Traversables

/**
 * @author clint
 * Nov 22, 2016
 */
final case class AsyncLocalChunkRunner(
    executionConfig: ExecutionConfig,
    maxNumJobs: Int = defaultMaxNumJobs)
    (implicit context: ExecutionContext) extends ChunkRunnerFor(EnvironmentType.Local) {

  import AsyncLocalChunkRunner._
  
  override def run(jobs: Set[LJob], shouldRestart: LJob => Boolean): Observable[Map[LJob, RunData]] = {
    if(jobs.isEmpty) { Observable.just(Map.empty) }
    else {
      import LocalJobStrategy.canBeRun
      
      require(
          jobs.forall(canBeRun), 
          s"Expected only LocalJobs, but found ${jobs.filterNot(canBeRun).mkString(",")}")
      
      def exec(job: LJob): Observable[RunData] = {
        Observable.from(executeSingle(executionConfig, job, shouldRestart))
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
      job: LJob, 
      shouldRestart: LJob => Boolean)(implicit executor: ExecutionContext): Future[RunData] = {
    
    job.transitionTo(JobStatus.Running)
    
    val processLogger = ProcessLoggers.forNamedJob(executionConfig, job)
    
    val result = LocalJobStrategy.execute(job, processLogger)

    import Futures.Implicits._
  
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
