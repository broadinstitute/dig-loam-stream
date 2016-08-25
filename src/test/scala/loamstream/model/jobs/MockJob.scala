package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import LJob.Result
import loamstream.model.jobs.JobState.{Running, Succeeded}
import loamstream.util.Sequence
import loamstream.util.ValueBox

/**
 * @author clint
 * date: Jun 2, 2016
 */
class MockJob(
    val toReturn: LJob.Result,
    val name: String,
    override val inputs: Set[LJob], 
    val outputs: Set[Output], 
    val delay: Int) extends LJob {
  
  val id: Int = MockJob.nextId()
  
  override def toString: String = s"'$name'(#$id, returning $toReturn)"
 
  private val equalityFields = Seq(toReturn)
  
  override def hashCode: Int = equalityFields.hashCode
  
  override def equals(other: Any): Boolean = other match {
    case that: MockJob => this.equalityFields == that.equalityFields
    case _ => false
  }
  
  override def execute(implicit context: ExecutionContext): Future[Result] = {
    count.mutate(_ + 1)

    stateRef() = Running

    if (delay > 0) {
      Thread.sleep(delay)
    }

    stateRef() = Succeeded

    Future.successful(toReturn)
  }
  
  private[this] val count = ValueBox(0)

  def executionCount = count.value

  def copy(
      toReturn: LJob.Result = this.toReturn,
      name: String = this.name,
      inputs: Set[LJob] = this.inputs,
      outputs: Set[Output] = this.outputs,
      delay: Int = this.delay): MockJob = new MockJob(toReturn, name, inputs, outputs, delay)

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}

object MockJob {
  def apply(
      toReturn: LJob.Result,
      name: String = nextId().toString, 
      inputs: Set[LJob] = Set.empty, 
      outputs: Set[Output] = Set.empty, 
      delay: Int = 0): MockJob = new MockJob(toReturn, name, inputs, outputs, delay)

  def unapply(job: LJob): Option[(LJob.Result, String, Set[LJob], Set[Output], Int)] = job match {
    case mj: MockJob => Some((mj.toReturn, mj.name, mj.inputs, mj.outputs, mj.delay))
    case _ => None
  }
  
  private[this] val ids: Sequence[Int] = Sequence()
  
  def nextId(): Int = ids.next()
}