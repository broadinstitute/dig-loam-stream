package loamstream.model.jobs

import loamstream.model.jobs.LJob.{Result, SimpleSuccess}

import scala.concurrent.{ExecutionContext, Future}

/**
 * @author Kaan Yuksel
 * date: Jun 2, 2016
 */
final case class NoOpJob(inputs: Set[LJob]) extends LJob {
  override protected def executeSelf(implicit context: ExecutionContext): Future[Result] = {
    Future.successful(SimpleSuccess("NoOp Job"))
  }

  override val outputs: Set[Output] = Set.empty
    
  override def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}
