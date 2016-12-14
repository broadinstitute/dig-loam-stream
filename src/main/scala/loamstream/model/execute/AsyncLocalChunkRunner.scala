package loamstream.model.execute

import scala.concurrent.ExecutionContext
import loamstream.model.jobs.LJob
import scala.concurrent.Future
import loamstream.model.jobs.JobState
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

  override def run(jobs: Set[LJob]): Observable[Map[LJob, JobState]] = {
    if(jobs.isEmpty) { Observable.just(Map.empty) }
    else {
      def exec(job: LJob): Observable[Map[LJob, JobState]] = Observable.from(executeSingle(job))

      val resultObservables: Seq[Observable[Map[LJob, JobState]]] = jobs.toSeq.map(exec)
        
      Observables.sequence(resultObservables).map(Maps.mergeMaps)
    }
  }
}

object AsyncLocalChunkRunner {
  def defaultMaxNumJobs: Int = Runtime.getRuntime.availableProcessors
}