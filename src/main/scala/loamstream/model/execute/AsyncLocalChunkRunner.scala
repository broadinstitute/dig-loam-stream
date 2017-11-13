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

/**
 * @author clint
 * Nov 22, 2016
 */
final case class AsyncLocalChunkRunner(
    maxNumJobs: Int = defaultMaxNumJobs)
    (implicit context: ExecutionContext) extends ChunkRunnerFor(EnvironmentType.Local) {

  import AsyncLocalChunkRunner._

  override def run(jobs: Set[LJob], shouldRestart: LJob => Boolean): Observable[Map[LJob, Execution]] = {
    if(jobs.isEmpty) { Observable.just(Map.empty) }
    else {
      import JobStrategy.canBeRunLocally
      
      require(
          jobs.forall(canBeRunLocally), 
          s"Expected only LocalJobs, but found ${jobs.filterNot(canBeRunLocally).mkString(",")}")
      
      def exec(job: LJob): Observable[(LJob, Execution)] = Observable.from(executeSingle(job, shouldRestart))

      val executionObservables: Seq[Observable[(LJob, Execution)]] = jobs.toSeq.map(exec)
        
      val sequenceObservable: Observable[Seq[(LJob, Execution)]] = Observables.sequence(executionObservables)
      
      sequenceObservable.foldLeft(Map.empty[LJob, Execution]) { _ ++ _ }
    }
  }
}

object AsyncLocalChunkRunner extends Loggable {
  def defaultMaxNumJobs: Int = Runtime.getRuntime.availableProcessors
  
  def executeSingle(
      job: LJob, 
      shouldRestart: LJob => Boolean)(implicit executor: ExecutionContext): Future[(LJob, Execution)] = {
    
    job.transitionTo(JobStatus.NotStarted)
    job.transitionTo(JobStatus.Running)
    
    val result = for {
      execution <- JobStrategy.localStrategyFor(job).execute
    } yield {
      job -> execution
    }

    import Futures.Implicits._
  
    result.withSideEffect(handleResultOfExecution(shouldRestart))
  }
  
  private[execute] def handleResultOfExecution(shouldRestart: LJob => Boolean)(tuple: (LJob, Execution)): Unit = {
    val (job, execution) = tuple
    
    trace(s"Handling result of execution: $job => $execution")
    
    val newStatus = determineFinalStatus(shouldRestart, execution.status, job)
    
    job.transitionTo(newStatus)
  }
  
  private[execute] def determineFinalStatus(
      shouldRestart: LJob => Boolean,
      newStatus: JobStatus,
      job: LJob): JobStatus = {

    if(newStatus.isFailure) ExecuterHelpers.determineFailureStatus(shouldRestart, newStatus, job) else newStatus
  }
}
