package loamstream.model.execute


import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobResult.CommandInvocationFailure
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.Output
import loamstream.util.FileWatchers
import loamstream.util.Loggable
import loamstream.model.jobs.RunData
import loamstream.model.LId

/**
 * @author clint
 * date: Jun 7, 2016
 */
object ExecuterHelpers extends Loggable {
  def noFailures[J <: LJob](m: Map[J, Execution]): Boolean = m.values.forall(_.status.isSuccess)
  
  def anyFailures[J <: LJob](m: Map[J, Execution]): Boolean = !noFailures(m)

  def determineFailureStatus(
      shouldRestart: LJob => Boolean, 
      failureStatus: JobStatus, 
      job: LJob): JobStatus = {
    
    val restarting = shouldRestart(job)
    
    if(restarting) {
      info(s"Restarting job $job")
      
      failureStatus
    } else {
       JobStatus.FailedPermanently
    }
  }
  
  def flattenTree(roots: Set[JobNode]): Set[JobNode] = {
    roots.foldLeft(roots) { (acc, job) =>
      val inputJobNodes = job.inputs
      
      inputJobNodes ++ flattenTree(inputJobNodes) ++ acc
    }
  }
  
  //TODO: TEST
  def statusAndResultFrom(t: Throwable): (JobStatus, JobResult) = {
    t.printStackTrace(System.err)
    
    (JobStatus.FailedWithException, CommandInvocationFailure(t))
  }
  
  //TODO: TEST
  def updateWithException(e: Execution, t: Throwable): Execution = {
    val (status, result) = statusAndResultFrom(t)
    
    e.withStatusAndResult(status, result)
  }
  
  //TODO: TEST
  def waitForOutputsOnly(
       job: LJob, 
       howLong: Duration)
      (implicit context: ExecutionContext): Future[_] = {
    
    val anyMissingOutputs = job.outputs.exists(_.isMissing)
    
    if(anyMissingOutputs) {
      error(s"Missing outputs for job ${job.id}! : ${job.outputs.toSeq.filter(_.isMissing)}")
    } else {
      trace(s"NO missing outputs for job ${job.id}")
    }
    
    val missingPaths = job.outputs.toSeq.collect { case o @ Output.PathOutput(p) if o.isMissing => p }
    
    val fileExistenceFutures = missingPaths.map(p => FileWatchers.waitForCreationOf(p, howLong))
    
    Future.sequence(fileExistenceFutures)
  }
  
  //TODO: TEST
  def waitForOutputs(doWait: Future[_], execution: => Execution)(implicit context: ExecutionContext): Future[Execution] = { 
    doWait.map { _ => execution }.recover { 
      case e => ExecuterHelpers.updateWithException(execution, e)
    }
  }
  
  //TODO: TEST
  def waitForOutputsAndMakeExecution(runData: RunData)(implicit context: ExecutionContext): Future[Execution] = {
    //TODO: XXX get from LocalConfig
    val howLong = {
      import scala.concurrent.duration._
      
      1.minute
    }
    
    if(runData.jobStatus.isSuccess) {
      waitForOutputs(waitForOutputsOnly(runData.job, howLong), runData.execution)
    } else {
      Future.successful(runData.execution)
    }
  }
}
