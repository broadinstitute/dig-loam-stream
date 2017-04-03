package loamstream.model.execute

import scala.concurrent.ExecutionContext
import loamstream.model.jobs.{Execution, LJob}
import loamstream.util.Maps
import AsyncLocalChunkRunner.defaultMaxNumJobs
import rx.lang.scala.Observable
import loamstream.util.Observables

/**
 * @author clint
 * Nov 22, 2016
 */
final case class AsyncLocalChunkRunner(
    maxNumJobs: Int = defaultMaxNumJobs)
    (implicit context: ExecutionContext) extends ChunkRunnerFor(ExecutionEnvironment.Local) {

  import ExecuterHelpers._

  override def run(jobs: Set[LJob]): Observable[Map[LJob, Execution]] = {
    if(jobs.isEmpty) { Observable.just(Map.empty) }
    else {
      def exec(job: LJob): Observable[Map[LJob, Execution]] = Observable.from(executeSingle(job))

      val executionObservables: Seq[Observable[Map[LJob, Execution]]] = jobs.toSeq.map(exec)
        
      Observables.sequence(executionObservables).map(Maps.mergeMaps)
    }
  }
}

object AsyncLocalChunkRunner {
  def defaultMaxNumJobs: Int = Runtime.getRuntime.availableProcessors
}