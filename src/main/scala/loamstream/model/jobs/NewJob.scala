package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.jobs.LJob.Result
import loamstream.util.SyncRef

/**
 * @author clint
 * date: Jul 25, 2016
 */
trait NewJob {
  
  def inputs: Set[NewJob]
  
  def outputs: Set[Output]
  
  private val statusRef: SyncRef[JobState] = SyncRef(JobState.NotStarted)
  
  def status: JobState = statusRef.getOrElse(JobState.Unknown)
  
  protected def isSuccess: Boolean = status.isFinished
  
  def execute(implicit context: ExecutionContext): Future[Result] = {
    val f = executeSelf
    
    import JobState._
    
    f.foreach { result =>
      statusRef() = if(result.isSuccess) Finished else Failed
    }
    
    f
  }
  
  protected def executeSelf(implicit context: ExecutionContext): Future[Result]
}

object NewJob {
  final case class Default(inputs: Set[NewJob] = Set.empty, outputs: Set[Output] = Set.empty) extends NewJob {
    override protected def executeSelf(implicit context: ExecutionContext): Future[Result] = {
      //TODO
      Future.successful(LJob.SimpleSuccess(""))
    }
  }
}