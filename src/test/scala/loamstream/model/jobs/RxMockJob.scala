package loamstream.model.jobs

import loamstream.util.ValueBox

import scala.concurrent.{ExecutionContext, Future}

/**
 * @author kaan
 *         date: Sep 14, 2016
 */
class RxMockJob(
    override val name: String,
    val inputs: Set[LJob],
    val outputs: Set[Output],
    override val dependencies: Set[LJob],
    delay: Int,
    val toReturn: JobState) extends LJob {

  private[this] val count = ValueBox(0)

  def executionCount = count.value

  override protected def executeSelf(implicit context: ExecutionContext): Future[JobState] = Future {
    trace(s"\t\tStarting job: $name")

    if (delay > 0) {
      Thread.sleep(delay)
    }

    trace(s"\t\t\tFinishing job: $name")

    count.mutate(_ + 1)

    toReturn
  }

  def copy(
      name: String = this.name,
      inputs: Set[LJob] = this.inputs,
      outputs: Set[Output] = this.outputs,
      dependencies: Set[LJob] = this.dependencies,
      delay: Int = this.delay,
      toReturn: JobState = this.toReturn): RxMockJob = {
    
    new RxMockJob(name, inputs, outputs, dependencies, delay, toReturn)
  }

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  override def toString: String = name
}

object RxMockJob {
  def apply(
      name: String,
      inputs: Set[LJob] = Set.empty,
      outputs: Set[Output] = Set.empty,
      dependencies: Set[LJob] = Set.empty,
      delay: Int = 0,
      toReturn: JobState = JobState.Succeeded): RxMockJob = {
    
    new RxMockJob(name, inputs, outputs, dependencies, delay, toReturn)
  }
}