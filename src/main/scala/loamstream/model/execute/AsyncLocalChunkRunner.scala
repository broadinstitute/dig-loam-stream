package loamstream.model.execute

import scala.concurrent.ExecutionContext
import loamstream.model.jobs.{Execution, LJob}
import loamstream.util.Maps
import AsyncLocalChunkRunner.defaultMaxNumJobs
import rx.lang.scala.Observable
import loamstream.util.Observables
import scala.concurrent.Future
import loamstream.model.jobs.JobStatus

/**
 * @author clint
 * Nov 22, 2016
 */
final case class AsyncLocalChunkRunner(
    maxNumJobs: Int = defaultMaxNumJobs)
    (implicit context: ExecutionContext) extends ChunkRunnerFor(ExecutionEnvironment.Local) {

  import ExecuterHelpers._

  override def run(jobs: Set[LJob], shouldRestart: LJob => Boolean): Observable[Map[LJob, Execution]] = {
    if(jobs.isEmpty) { Observable.just(Map.empty) }
    else {
      def exec(job: LJob): Observable[(LJob, Execution)] = Observable.from(executeSingle(job, shouldRestart))

      val executionObservables: Seq[Observable[(LJob, Execution)]] = jobs.toSeq.map(exec)
        
      val sequenceObservable: Observable[Seq[(LJob, Execution)]] = Observables.sequence(executionObservables)
      
      sequenceObservable.foldLeft(Map.empty[LJob, Execution]) { _ ++ _ }
    }
  }
}

object AsyncLocalChunkRunner {
  def defaultMaxNumJobs: Int = Runtime.getRuntime.availableProcessors
}
