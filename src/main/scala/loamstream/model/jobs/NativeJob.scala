package loamstream.model.jobs
import loamstream.model.jobs.LJob.Result
import loamstream.util.EvalLaterBox

import scala.concurrent.{ExecutionContext, Future}

/** Job defined by Loam code */
final case class NativeJob[T](exprBox: EvalLaterBox[T], inputs: Set[LJob] = Set.empty) extends LJob{
  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  override def execute(implicit context: ExecutionContext): Future[Result] =
    exprBox.evalFuture.map(LJob.ValueSuccess(_, exprBox.typeBox))
}
