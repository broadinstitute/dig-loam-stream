package loamstream.model.jobs

import loamstream.model.execute.Settings
import loamstream.model.LId
import scala.util.Success
import scala.util.Failure
import loamstream.util.Loggable
import monix.eval.Task
import scala.util.control.NonFatal

/**
 * @author clint
 * Dec 19, 2019
 */
final case class NativeJob(
    body: () => Any,
    initialSettings: Settings,
    dependencies: Set[JobNode] = Set.empty,
    protected override val successorsFn: () => Set[JobNode] = () => Set.empty,
    inputs: Set[DataHandle] = Set.empty,
    outputs: Set[DataHandle] = Set.empty,
    nameOpt: Option[String] = None) extends LocalJob with JobNode.LazySucessors with Loggable {
  
  override def name: String = nameOpt.getOrElse(id.toString)
  
  override def equals(other: Any): Boolean = other match {
    case that: NativeJob => this.id == that.id
    case _ => false
  }
  
  override def hashCode: Int = id.hashCode
  
  override def toString: String = {
    s"${getClass.getSimpleName}('${name}' dependencies: ${dependencies.size} " +
    s"inputs: ${inputs.size} outputs: ${outputs.size}, $initialSettings)"
  }
  
  override def execute: Task[RunData] = {
    def onSuccess = RunData(
        job = this,
        settings = initialSettings,
        jobStatus = JobStatus.Succeeded,
        jobResult = Some(JobResult.Success),
        resourcesOpt = None,
        jobDirOpt = None,
        terminationReasonOpt = None)
    
    def onFailure(e: Throwable) = {
      error(s"Error running native job $id", e)
      
      RunData(
        job = this,
        settings = initialSettings,
        jobStatus = JobStatus.Failed,
        jobResult = Some(JobResult.FailureWithException(e)),
        resourcesOpt = None,
        jobDirOpt = None,
        terminationReasonOpt = None)
    }
        
    Task(body()).map(_ => onSuccess).onErrorRecover {
      case NonFatal(e) => onFailure(e)
    }
  }
}
