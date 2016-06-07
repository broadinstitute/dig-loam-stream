package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import LJob.Result

/**
 * @author clint
 * date: Jun 2, 2016
 */
final case class MockLJob(inputs: Set[LJob], toReturn: LJob.Result) extends LJob {
  val id: Int = MockLJob.nextId()
  
  override def toString: String = s"#$id (returning $toReturn)"
  
  override def execute(implicit context: ExecutionContext): Future[Result] = Future.successful(toReturn)

  override def withInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}

object MockLJob {
  private[this] var latestId: Int = 0
  
  private[this] val lock = new AnyRef
  
  def nextId(): Int = lock.synchronized {
    try { latestId }
    finally { latestId += 1 }
  }
}