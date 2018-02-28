package loamstream.model.jobs

import loamstream.TestHelpers

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import loamstream.util.Sequence
import loamstream.util.ValueBox
import loamstream.model.execute.Environment
import loamstream.util.Futures
import loamstream.model.execute.Resources

/**
 * @author clint
 * date: Jun 2, 2016
 */
abstract class MockJob(
    override val name: String,
    override val inputs: Set[JobNode],
    override val outputs: Set[Output],
    val delay: Int) extends LocalJob {

  def toReturn: RunData
  
  override def executionEnvironment: Environment = TestHelpers.env
  
  override def toString: String = {
    s"'$name'(#$id, returning $toReturn, ${inputs.size} dependencies)"
  }
 
  //NB: Previous versions defined equals() and hashCode() only in terms of 'toReturn', which caused problems;
  //switched back to reference equality.

  override def execute(implicit context: ExecutionContext): Future[RunData] = {
    count.mutate(_ + 1)

    if (delay > 0) {
      Thread.sleep(delay)
    }
    
    import Futures.Implicits._
    
    Future.successful(toReturn)
  }
  
  private[this] val count = ValueBox(0)

  def executionCount = count.value

  def copy(
      name: String = this.name,
      inputs: Set[JobNode] = this.inputs,
      outputs: Set[Output] = this.outputs,
      delay: Int = this.delay): MockJob = new MockJob.Constant(this.toReturn, name, inputs, outputs, delay)
}

object MockJob {
  private final class Constant(
      override val toReturn: RunData,
      override val name: String,
      override val inputs: Set[JobNode],
      override val outputs: Set[Output],
      override val delay: Int) extends MockJob(name, inputs, outputs, delay)
  
  class FromJobFn(
      toReturnFn: MockJob => RunData,
      override val name: String,
      override val inputs: Set[JobNode],
      override val outputs: Set[Output],
      override val delay: Int) extends MockJob(name, inputs, outputs, delay) {
    
    override lazy val toReturn: RunData = toReturnFn(this)
  }
  
  import TestHelpers._

  def apply(toReturn: RunData): MockJob = {
    new Constant(
        toReturn = toReturn,
        name = LJob.nextId().toString,
        inputs = Set.empty,
        outputs = Set.empty,
        delay = 0)
  }

  def apply(jobResultToReturn: JobResult): MockJob = {
    new FromJobFn(
        toReturnFn = job => runDataFromResult(job, jobResultToReturn),
        name = LJob.nextId().toString,
        inputs = Set.empty,
        outputs = Set.empty,
        delay = 0)
  }

  def apply(
      jobResult: JobResult,
      resources: Option[Resources],
      outputStreams: Option[OutputStreams]): MockJob = {
    
    new FromJobFn(
        toReturnFn = job => runDataFrom(job, jobResult.toJobStatus, Option(jobResult), resources, outputStreams),
        name = LJob.nextId().toString,
        inputs = Set.empty,
        outputs = Set.empty,
        delay = 0)
  } 
  
  def apply(
      toReturn: JobStatus,
      name: String = LJob.nextId().toString,
      inputs: Set[JobNode] = Set.empty,
      outputs: Set[Output] = Set.empty,
      delay: Int = 0): MockJob = {
    
    new FromJobFn(
        job => runDataFromStatus(job, toReturn),
        name,
        inputs,
        outputs,
        delay)
  }

  def unapply(job: LJob): Option[(RunData, String, Set[JobNode], Set[Output], Int)] = job match {
    case mj: MockJob => Some((mj.toReturn, mj.name, mj.inputs, mj.outputs, mj.delay))
    case _ => None
  }
}
