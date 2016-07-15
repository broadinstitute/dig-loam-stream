package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import LJob.Result

/**
 * @author clint
 * date: Jun 2, 2016
 */
final class MockLJob(override val inputs: Set[LJob], val toReturn: LJob.Result) extends LJob {
  
  val id: Int = MockLJob.nextId()
  
  override def toString: String = s"#$id (returning $toReturn)"
 
  private val equalityFields = Seq(/*inputs, */toReturn)
  
  override def hashCode: Int = equalityFields.hashCode
  
  override def equals(other: Any): Boolean = other match {
    case that: MockLJob => this.equalityFields == that.equalityFields
    case _ => false
  }
  
  override def execute(implicit context: ExecutionContext): Future[Result] = Future.successful(toReturn)

  def copy(inputs: Set[LJob] = this.inputs, toReturn: LJob.Result = this.toReturn): MockLJob = {
    new MockLJob(inputs, toReturn)
  }
  
  override protected def doWithInputs(newInputs: Set[LJob]): LJob = new MockLJob(newInputs, toReturn)
}

object MockLJob {
  def apply(inputs: Set[LJob], toReturn: LJob.Result): MockLJob = new MockLJob(inputs, toReturn)

  def unapply(job: LJob): Option[(Set[LJob], LJob.Result)] = job match {
    case mj: MockLJob => Some(mj.inputs -> mj.toReturn)
    case _ => None
  }
  
  private[this] var latestId: Int = 0
  
  private[this] val lock = new AnyRef
  
  def nextId(): Int = lock.synchronized {
    try { latestId }
    finally { latestId += 1 }
  }
}