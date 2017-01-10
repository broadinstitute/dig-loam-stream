package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.execute.ExecutionEnvironment
import loamstream.util.Futures
import loamstream.util.Observables
import loamstream.util.ValueBox


/**
 * @author kaan
 * @author clint
 * date: Sep 15, 2016
 */
final case class RxMockJob(
    override val name: String,
    inputs: Set[LJob] = Set.empty,
    outputs: Set[Output] = Set.empty,
    runsAfter: Set[RxMockJob] = Set.empty,
    fakeExecutionTimeInMs: Int = 0,
    toReturn: JobState = JobState.Succeeded) extends LJob {

  override def executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.Local
  
  private[this] val count = ValueBox(0)

  def executionCount = count.value

  private def waitIfNecessary(): Unit = {
    if (runsAfter.nonEmpty) {
      import loamstream.util.ObservableEnrichments._
      val finalDepStates = Observables.sequence(runsAfter.toSeq.map(_.lastState))

      Futures.waitFor(finalDepStates.firstAsFuture)
    }
  }

  private def delayIfNecessary(): Unit = {
    if (fakeExecutionTimeInMs > 0) {
      Thread.sleep(fakeExecutionTimeInMs)
    }
  }

  override def execute(implicit context: ExecutionContext): Future[JobState] = {
    Future(waitIfNecessary()).flatMap(_ => super.execute)
  }

  override protected def executeSelf(implicit context: ExecutionContext): Future[JobState] = Future {

    trace(s"\t\tStarting job: $name")

    delayIfNecessary()

    trace(s"\t\t\tFinishing job: $name")

    count.mutate(_ + 1)

    toReturn
  }

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  override def toString: String = name
}
