package loamstream.model.execute


import scala.concurrent.duration.Duration

import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobResult.CommandInvocationFailure
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.DataHandle
import loamstream.util.Loggable
import loamstream.model.jobs.RunData
import loamstream.model.LId
import loamstream.conf.LoamConfig
import loamstream.util.Functions
import scala.util.Try
import loamstream.util.FileMonitor
import scala.util.control.NonFatal
import scala.collection.compat._
import monix.eval.Task

/**
 * @author clint
 * date: Jun 7, 2016
 */
object ExecuterHelpers extends Loggable {

  def flattenTree(roots: Iterable[JobNode], field: JobNode => Set[JobNode] = _.dependencies): Iterable[JobNode] = {
    roots.foldLeft(roots) { (acc, job) =>
      val jobNodes = field(job)
      
      jobNodes ++ flattenTree(jobNodes, field) ++ acc
    }
  }
  
  def statusAndResultFrom(t: Throwable): (JobStatus, JobResult) = {
    (JobStatus.FailedWithException, CommandInvocationFailure(t))
  }
  
  /**
   * Return a new Execution based on `e`, with `status` and `result` fields in the
   * returned Execution derived from `t`.
   * @see statusAndResultFrom
   */
  def updateWithException(e: Execution, t: Throwable): Execution = {
    val (status, result) = statusAndResultFrom(t)
    
    e.withStatusAndResult(status, result)
  }
  
  /**
   * Given a job, return a Task that completes when all of the job's outputs are available.
   * If the job's outputs are all initially available, return an eagerly-evaluated Task.
   * If any of the job's outputs are missing, wait for them via FileWatchers.waitForCreationOf.
   * If `howlLong` elapses without the outputs appearing, a failed Task is returned. 
   * NOTE: UriOutputs are implicitly ignored (!) :\
   */
  def waitForOutputsOnly(
       job: LJob, 
       fileMonitor: FileMonitor): Task[_] = {
    
    val anyMissingOutputs = job.outputs.exists(_.isMissing)
    
    if(anyMissingOutputs) {
      val missingOutputs = job.outputs.to(Seq).filter(_.isMissing)
      
      error(s"Will wait for these missing outputs for job ${job.id} : ${missingOutputs}")
      
      //TODO: Support UriOutputs!!
      val missingPaths = missingOutputs.collect { case o: DataHandle.PathHandle => o.path }
    
      val fileExistenceTasks = missingPaths.map(fileMonitor.waitForCreationOf).map(Task.fromFuture)
    
      Task.parSequence(fileExistenceTasks)
    } else {
      trace(s"NO missing outputs for job ${job.id}")
      
      Task.now(())
    }
  }
  
  /**
   * After the Task `doWait` completes, make an Execution by evaluating `execution`.  
   * If an exception is thrown while evaluating `execution`, evaluate `executionInCaseOfFailure` to make
   * an Execution.
   * If an exception is thrown while waiting (ie, doWait is a failed Task), update the produced Execution
   * with a JobStatus and JobResult to indicate the failure.
   */
  private[execute] def doWaiting(
      doWait: Task[_], 
      execution: => Execution,
      executionInCaseOfFailure: => Execution): Task[Execution] = {
    
    lazy val executionToUse = try {
      trace("Making Execution from RunData...")
      
      val result = execution
      
      trace(s"Made $result from RunData")
      
      result
    } catch {
      case NonFatal(e) => {
        trace(s"Making fallback execution due to ${e.getClass.getName}...", e)
        
        val result = executionInCaseOfFailure
        
        trace(s"Made fallback Execution: $result")
        
        result
      }
    }
    
    doWait.map { _ => executionToUse }.onErrorRecover { 
      case e => {
        error(s"Error waiting for outputs and making Execution: ", e)
        
        ExecuterHelpers.updateWithException(executionToUse, e)
      }
    }
  }
  
  /**
   * Take a RunData and a timeout, and return a Task[Execution] 
   * If the RunData represents a successful job run, wait for any missing outputs, and turn the RunData into an 
   * Execution.
   * If the RunData represents a failed job run, turn the RunData into an Execution immediately and return it in a 
   * Task.
   */
  def waitForOutputsAndMakeExecution(
      runData: RunData, 
      fileMonitor: FileMonitor): Task[Execution] = {
    
    //TODO: Revisit this: should we should wait in all cases?
    if(runData.jobStatus.isSuccess) {
      doWaiting(
          waitForOutputsOnly(runData.job, fileMonitor), 
          runData.toExecution,
          Execution.from(runData.job, JobStatus.Failed, terminationReason = None))
    } else {
      Task(runData.toExecution)
    }
  }
}
