package loamstream.googlecloud

import loamstream.model.execute.ChunkRunner
import loamstream.model.jobs.LJob
import loamstream.model.execute.ExecutionEnvironment
import loamstream.model.execute.ChunkRunnerFor
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import loamstream.model.jobs.JobState
import loamstream.util.Terminable
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.execute.AsyncLocalChunkRunner

/**
 * @author clint
 * Nov 28, 2016
 */
final case class GoogleCloudChunkRunner(
    client: DataProcClient, 
    delegate: ChunkRunner = AsyncLocalChunkRunner(1)) extends 
  ChunkRunnerFor(ExecutionEnvironment.Google) with Terminable {
  
  override def maxNumJobs: Int = delegate.maxNumJobs
  
  override def stop(): Unit = client.deleteClusterIfRunning()
  
  override def run(jobs: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, JobState]] = {
    if(jobs.nonEmpty) {
      client.doWithCluster {
        //TODO: Enforce a serial-execution contract somehow?  We likely don't want multilpe jobs running on the same 
        //cluster simultaneously.
        delegate.run(jobs)
      }
    } else {
      Future.successful(Map.empty)
    }
  }
  
}