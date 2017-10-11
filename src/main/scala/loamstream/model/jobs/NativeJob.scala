package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.conf.LocalSettings
import loamstream.model.execute.Environment
import loamstream.util.EvalLaterBox
import loamstream.model.execute.EnvironmentType


/** Job defined by Loam code */
final case class NativeJob[T](
    exprBox: EvalLaterBox[T], 
    inputs: Set[LJob] = Set.empty,
    outputs: Set[Output] = Set.empty) extends LJob {
  
  override def name: String = s"${getClass.getSimpleName}#${id}(?,?,?)" 
  
  //TODO: Can we say this for all NativeJobs?
  override def executionEnvironment: Environment = Environment.Local
  
  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  override def execute(implicit executionContext: ExecutionContext): Future[Execution] = {
    exprBox.evalFuture.map { value =>
      Execution(id = None,
                envType = EnvironmentType.Local,
                cmd = None,
                settings = LocalSettings,
                status = JobStatus.Succeeded,
                result = Some(JobResult.ValueSuccess(value, exprBox.typeBox)), // TODO: Is this right?
                resources = None,
                outputs = outputs.map(_.toOutputRecord))
    }
  }
}
