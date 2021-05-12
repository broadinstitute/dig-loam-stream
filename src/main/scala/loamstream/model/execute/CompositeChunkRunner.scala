package loamstream.model.execute

import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.Throwables
import monix.reactive.Observable

/**
 * @author clint
 * Nov 22, 2016
 */
final case class CompositeChunkRunner(components: Seq[ChunkRunner]) extends ChunkRunner with Loggable {
  
  override def canRun(job: LJob): Boolean = components.exists(_.canRun(job))
  
  override def run(
      jobs: Iterable[LJob], 
      jobOracle: JobOracle): Observable[(LJob, RunData)] = {
    
    require(jobs.forall(canRun), s"Don't know how to run ${jobs.filterNot(canRun)}")
    
    val byRunner: Iterable[(ChunkRunner, Iterable[LJob])] = components.map { runner => 
      runner -> jobs.filter(runner.canRun) 
    }
    
    val resultObservables = for {
      (runner, jobsForRunner) <- byRunner
    } yield {
      runner.run(jobsForRunner, jobOracle)
    }
    
    Observables.merge(resultObservables)
  }
  
  override def stop(): Unit = {
    for {
      component <- components
    } {
      Throwables.quietly("Error shutting down: ")(component.stop())
    }
  }
}
