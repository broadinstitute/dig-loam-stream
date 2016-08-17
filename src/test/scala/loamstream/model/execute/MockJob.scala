package loamstream.model.execute

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.Output
import loamstream.util.ValueBox


/**
 * @author clint
 * date: Aug 12, 2016
 */
final case class MockJob(
    name: String,
    inputs: Set[LJob] = Set.empty,
    outputs: Set[Output] = Set.empty,
    delay: Int = 0) extends LJob {

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  private[this] val count = ValueBox(0)

  def executionCount = count.value

  override protected def executeSelf(implicit context: ExecutionContext): Future[Result] = {
    count.mutate(_ + 1)
    Thread.sleep(delay)
    Future.successful(LJob.SimpleSuccess(name))
  }
}