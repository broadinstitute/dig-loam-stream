package loamstream.model.execute

import loamstream.model.jobs.{Execution, LJob}
import rx.lang.scala.Observable
import loamstream.util.Observables

/**
 * @author clint
 * Nov 22, 2016
 */
final case class CompositeChunkRunner(components: Seq[ChunkRunner]) extends ChunkRunner {
  
  override def maxNumJobs: Int = components.map(_.maxNumJobs).sum
  
  override def canRun(job: LJob): Boolean = components.exists(_.canRun(job))
  
  override def run(jobs: Set[LJob]): Observable[Map[LJob, Execution]] = {
    
    require(jobs.forall(canRun), s"Don't know how to run ${jobs.filterNot(canRun)}")
    
    val byRunner: Map[ChunkRunner, Set[LJob]] = components.map { runner => 
      runner -> jobs.filter(runner.canRun) 
    }.toMap
    
    val resultObservables = for {
      (runner, jobsForRunner) <- byRunner
    } yield {
      runner.run(jobsForRunner)
    }
    
    Observables.reduceMaps(resultObservables)
  }
}