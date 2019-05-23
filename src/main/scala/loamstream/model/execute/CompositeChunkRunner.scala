package loamstream.model.execute

import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.Throwables
import rx.lang.scala.Observable

/**
 * @author clint
 * Nov 22, 2016
 */
final case class CompositeChunkRunner(components: Seq[ChunkRunner]) extends ChunkRunner with Loggable {
  
  override def maxNumJobs: Int = components.map(_.maxNumJobs).sum
  
  override def canRun(job: LJob): Boolean = components.exists(_.canRun(job))
  
  override def run(jobs: Set[LJob], jobOracle: JobOracle, shouldRestart: LJob => Boolean): Observable[Map[LJob, RunData]] = {
    
    require(jobs.forall(canRun), s"Don't know how to run ${jobs.filterNot(canRun)}")
    
    val byRunner: Map[ChunkRunner, Set[LJob]] = components.map { runner => 
      runner -> jobs.filter(runner.canRun) 
    }.toMap
    
    val resultObservables = for {
      (runner, jobsForRunner) <- byRunner
    } yield {
      runner.run(jobsForRunner, jobOracle, shouldRestart)
    }
    
    val z: Map[LJob, RunData] = Map.empty
    
    //NB: Note the use of scan() here.  It ensures that an item is emitted for a job as soon as that job finishes,     
    //instead of only once when all the jobs in a chunk finish.
    Observables.merge(resultObservables).scan(z)(_ ++ _).distinct
  }
  
  override def stop(): Unit = {
    for {
      component <- components
    } {
      Throwables.quietly("Error shutting down: ")(component.stop())
    }
  }
}
