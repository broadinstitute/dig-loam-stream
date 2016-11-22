package loamstream.model.jobs

import scala.concurrent.{ ExecutionContext, Future }

import loamstream.util.EvalLaterBox
import loamstream.model.execute.ExecutionEnvironment

/** Job defined by Loam code */
final case class NativeJob[T](
    exprBox: EvalLaterBox[T], 
    inputs: Set[LJob] = Set.empty,
    outputs: Set[Output] = Set.empty) extends LJob {
  
  //TODO: Can we say this for all NativeJobs?
  override def executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.Local
  
  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  override protected def executeSelf(implicit executionContext: ExecutionContext): Future[JobState] =
    exprBox.evalFuture.map(JobState.ValueSuccess(_, exprBox.typeBox))
}
