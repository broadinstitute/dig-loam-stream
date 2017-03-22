package loamstream.model.execute

import scala.concurrent.ExecutionContext
import loamstream.model.jobs.{JobResult, LJob}
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

  override def run(jobs: Set[LJob]): Observable[Map[LJob, JobResult]] = {
    if(jobs.isEmpty) { Observable.just(Map.empty) }
    else {
      def exec(job: LJob): Observable[Map[LJob, JobResult]] = Observable.from(executeSingle(job))

      val resultObservables: Seq[Observable[Map[LJob, JobResult]]] = jobs.toSeq.map(exec)
        
      Observables.sequence(resultObservables).map(Maps.mergeMaps)
    }
  }
}

object AsyncLocalChunkRunner {
  def defaultMaxNumJobs: Int = Runtime.getRuntime.availableProcessors
}