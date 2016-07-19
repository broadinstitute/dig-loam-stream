package loamstream.model.jobs

import loamstream.model.jobs.LJob.{Result, SimpleSuccess}

import scala.concurrent.{ExecutionContext, Future}

/**
 * @author Kaan Yuksel
 * date: Jun 2, 2016
 */
final case class NoOpJob(inputs: Set[LJob]) extends LJob {
  override def execute(implicit context: ExecutionContext): Future[Result] =
    Future.successful(SimpleSuccess("NoOp Job"))

  override def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}
