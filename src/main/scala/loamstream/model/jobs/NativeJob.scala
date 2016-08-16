package loamstream.model.jobs

import loamstream.model.jobs.LJob.Result
import loamstream.util.EvalLaterBox

import scala.concurrent.{ExecutionContext, Future}

/** Job defined by Loam code */
final case class NativeJob[T](
    exprBox: EvalLaterBox[T], 
    inputs: Set[LJob] = Set.empty,
    outputs: Set[Output] = Set.empty) extends LJob {
  
  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
  
  override protected def doWithOutputs(newOutputs: Set[Output]): LJob = copy(outputs = newOutputs)

  override def executeSelf(implicit executionContext: ExecutionContext): Future[Result] =
    exprBox.evalFuture.map(LJob.ValueSuccess(_, exprBox.typeBox))
}
