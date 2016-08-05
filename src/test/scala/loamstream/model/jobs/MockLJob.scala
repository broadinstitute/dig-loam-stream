package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import LJob.Result

/**
 * @author clint
 * date: Jun 2, 2016
 */
final class MockLJob(
    override val inputs: Set[LJob], 
    val outputs: Set[Output], 
    val toReturn: LJob.Result) extends LJob {
  
  val id: Int = MockLJob.nextId()
  
  override def toString: String = s"#$id (returning $toReturn)"
 
  private val equalityFields = Seq(/*inputs, */toReturn)
  
  override def hashCode: Int = equalityFields.hashCode
  
  override def equals(other: Any): Boolean = other match {
    case that: MockLJob => this.equalityFields == that.equalityFields
    case _ => false
  }
  
  override protected def executeSelf(implicit context: ExecutionContext): Future[Result] = Future.successful(toReturn)

  def copy(
      inputs: Set[LJob] = this.inputs, 
      outputs: Set[Output] = this.outputs, 
      toReturn: LJob.Result = this.toReturn): MockLJob = new MockLJob(inputs, outputs, toReturn)
  
  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}

object MockLJob {
  def apply(inputs: Set[LJob], outputs: Set[Output], toReturn: LJob.Result): MockLJob = {
    new MockLJob(inputs, outputs, toReturn)
  }

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