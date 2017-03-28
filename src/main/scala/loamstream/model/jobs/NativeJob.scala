package loamstream.model.jobs

import scala.concurrent.{ExecutionContext, Future}
import loamstream.util.EvalLaterBox
import loamstream.model.execute.{ExecutionEnvironment, LocalSettings}

/** Job defined by Loam code */
final case class NativeJob[T](
    exprBox: EvalLaterBox[T], 
    inputs: Set[LJob] = Set.empty,
    outputs: Set[Output] = Set.empty) extends LJob {
  
  //TODO: Can we say this for all NativeJobs?
  override def executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.Local
  
  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  override protected def executeSelf(implicit executionContext: ExecutionContext): Future[Execution] = {
    exprBox.evalFuture.map { value =>
      Execution(executionEnvironment,
        None,
        LocalSettings(),
        JobStatus.Succeeded,
        Some(JobResult.ValueSuccess(value, exprBox.typeBox)), // TODO: Is this right?
        None,
        outputs.map(_.toOutputRecord))
    }
  }
}
