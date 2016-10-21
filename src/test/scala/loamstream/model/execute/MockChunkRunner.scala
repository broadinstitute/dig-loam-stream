package loamstream.model.execute

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.conf.UgerConfig
import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob
import loamstream.util.ValueBox

/**
 * @author clint
 * Oct 12, 2016
 */
final case class MockChunkRunner(delegate: ChunkRunner, maxNumJobs: Int) extends ChunkRunner {
  val chunks: ValueBox[Seq[Set[LJob]]] = ValueBox(Vector.empty)

  override def run(chunk: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, JobState]] = {
    chunks.mutate(_ :+ chunk)

    delegate.run(chunk)
  }
}

object MockChunkRunner {
  //TODO: ???
  def apply(delegate: ChunkRunner): MockChunkRunner = {
    val maxNumJobs = UgerConfig.fromFile("src/test/resources/loamstream-test.conf").get.ugerMaxNumJobs
    
    new MockChunkRunner(delegate, maxNumJobs)
  }
}