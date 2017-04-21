package loamstream.model.jobs

import scala.concurrent.{ExecutionContext, Future}
import loamstream.model.execute.{ExecutionEnvironment, LocalSettings}
import loamstream.model.jobs.JobStatus.Succeeded

/**
 * @author Kaan Yuksel
 * date: Jun 2, 2016
 */
//TODO: Get rid of this
final case class NoOpJob(inputs: Set[LJob]) extends LJob {
  override def execute(implicit context: ExecutionContext): Future[Execution] = {
    val noOpExecution = Execution(env = ExecutionEnvironment.Local,
                                  cmd = None,
                                  settings = LocalSettings(),
                                  status = Succeeded,
                                  result = None,
                                  resources = None,
                                  outputs = Set.empty[OutputRecord])
    Future.successful(noOpExecution)
  }
  
  override def name: String = s"${getClass.getSimpleName}#${id}(${inputs.size} dependencies)"
  
  override def executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.Local

  override val outputs: Set[Output] = Set.empty
    
  override def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}
