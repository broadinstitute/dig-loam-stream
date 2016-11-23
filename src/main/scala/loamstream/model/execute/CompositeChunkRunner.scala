package loamstream.model.execute

import scala.concurrent.ExecutionContext
import loamstream.model.jobs.LJob
import scala.concurrent.Future
import loamstream.model.jobs.JobState
import loamstream.util.Maps

/**
 * @author clint
 * Nov 22, 2016
 */
final case class CompositeChunkRunner(components: Seq[ChunkRunner]) extends ChunkRunner {
  
  override def maxNumJobs: Int = components.map(_.maxNumJobs).sum
  
  override def canRun(job: LJob): Boolean = components.exists(_.canRun(job))
  
  override def run(leaves: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, JobState]] = {
    
    require(leaves.forall(canRun), s"Don't know how to run ${leaves.filterNot(canRun)}")
    
    val byRunner: Map[ChunkRunner, Set[LJob]] = components.map { runner => 
      runner -> leaves.filter(runner.canRun) 
    }.toMap
    
    val resultFutures = for {
      (runner, jobs) <- byRunner
    } yield {
      runner.run(jobs)
    }
    
    Future.sequence(resultFutures).map(Maps.mergeMaps)
  }
}