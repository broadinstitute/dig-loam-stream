package loamstream.model.jobs

import loamstream.TestHelpers

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import loamstream.util.Sequence
import loamstream.util.ValueBox
import loamstream.model.execute.Environment
import loamstream.util.Futures
import loamstream.model.execute.Resources
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.Settings

/**
 * @author clint
 * date: Jun 2, 2016
 */
abstract class MockJob(
    override val name: String,
    override val dependencies: Set[JobNode],
    override val inputs: Set[DataHandle],
    override val outputs: Set[DataHandle],
    val delay: Int) extends LocalJob {

  def toReturn: RunData
  
  override def initialSettings: Settings = LocalSettings
  
  override def toString: String = {
    s"'$name'(#$id, returning $toReturn, ${dependencies.size} dependencies)"
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
      dependencies: Set[JobNode] = this.dependencies,
      inputs: Set[DataHandle] = this.inputs,
      outputs: Set[DataHandle] = this.outputs,
      delay: Int = this.delay): MockJob = {
    
    new MockJob.Constant(this.toReturn, name, dependencies, inputs, outputs, delay)
  }
}

object MockJob {
  private final class Constant(
      override val toReturn: RunData,
      override val name: String,
      override val dependencies: Set[JobNode],
      override val inputs: Set[DataHandle],
      override val outputs: Set[DataHandle],
      override val delay: Int) extends MockJob(name, dependencies, inputs, outputs, delay)
  
  class FromJobFn(
      toReturnFn: MockJob => RunData,
      override val name: String,
      override val dependencies: Set[JobNode],
      override val inputs: Set[DataHandle],
      override val outputs: Set[DataHandle],
      override val delay: Int) extends MockJob(name, dependencies, inputs, outputs, delay) {
    
    override lazy val toReturn: RunData = toReturnFn(this)
  }
  
  import TestHelpers._

  def apply(toReturn: RunData): MockJob = {
    new Constant(
        toReturn = toReturn,
        name = LJob.nextId().toString,
        dependencies = Set.empty,
        inputs = Set.empty,
        outputs = Set.empty,
        delay = 0)
  }

  def apply(jobResultToReturn: JobResult): MockJob = {
    new FromJobFn(
        toReturnFn = job => runDataFromResult(job, LocalSettings, jobResultToReturn),
        name = LJob.nextId().toString,
        dependencies = Set.empty,
        inputs = Set.empty,
        outputs = Set.empty,
        delay = 0)
  }

  def apply(
      jobResult: JobResult,
      resources: Option[Resources],
      outputStreams: Option[OutputStreams]): MockJob = {
    
    def makeRunData(job: LJob) = {
      runDataFrom(job, LocalSettings, jobResult.toJobStatus, Option(jobResult), resources, outputStreams)
    }
    
    new FromJobFn(
        toReturnFn = makeRunData,
        name = LJob.nextId().toString,
        dependencies = Set.empty,
        inputs = Set.empty,
        outputs = Set.empty,
        delay = 0)
  } 
  
  def apply(
      toReturn: JobStatus,
      name: String = LJob.nextId().toString,
      dependencies: Set[JobNode] = Set.empty,
      inputs: Set[DataHandle] = Set.empty,
      outputs: Set[DataHandle] = Set.empty,
      delay: Int = 0): MockJob = {
    
    new FromJobFn(
        job => runDataFromStatus(job, LocalSettings, toReturn),
        name,
        dependencies,
        inputs,
        outputs,
        delay)
  }

  def unapply(job: LJob): Option[(RunData, String, Set[JobNode], Set[DataHandle], Int)] = job match {
    case mj: MockJob => Some((mj.toReturn, mj.name, mj.dependencies, mj.outputs, mj.delay))
    case _ => None
  }
}
