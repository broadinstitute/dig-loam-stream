package loamstream.model.jobs

import scala.concurrent.{ExecutionContext, Future}

/**
 * @author Kaan Yuksel
 * date: Jun 2, 2016
 */
final case class NoOpJob(inputs: Set[LJob]) extends LJob {
  override protected def executeSelf(implicit context: ExecutionContext): Future[JobState] = {
    Future.successful(JobState.Succeeded)
  }

  override val outputs: Set[Output] = Set.empty
    
  override def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}
