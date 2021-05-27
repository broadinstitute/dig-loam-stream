package loamstream.drm.slurm

import loamstream.drm.JobSubmitter
import loamstream.model.execute.DrmSettings
import loamstream.drm.DrmTaskArray
import monix.reactive.Observable
import loamstream.drm.DrmSubmissionResult
import loamstream.util.CommandInvoker
import loamstream.util.Traversables
import loamstream.drm.DrmJobWrapper
import loamstream.drm.DrmTaskId
import loamstream.util.RunResults
import loamstream.util.Loggable
import scala.util.Failure
import scala.util.Success
import monix.execution.Scheduler
import loamstream.util.Processes
import scala.collection.compat._
import loamstream.conf.SlurmConfig

/**
 * @author clint
 * May 18, 2021
 */
final class SbatchJobSubmitter private[slurm] (
    submissionFn: CommandInvoker.Async[SbatchJobSubmitter.Params]) extends JobSubmitter with Loggable {
    
  import SbatchJobSubmitter._
  
  //TODO: Factor out
  override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): Observable[DrmSubmissionResult] = {
    val runAttemptObs = Observable.fromTask(submissionFn(drmSettings, taskArray)).map(toDrmSubmissionResult(taskArray))
    
    runAttemptObs.onErrorHandle(DrmSubmissionResult.SubmissionFailure(_))
  }
  
  override def stop(): Unit = ()
  
  //TODO: Factor out
  private[slurm] def toDrmSubmissionResult(taskArray: DrmTaskArray)(runResults: RunResults): DrmSubmissionResult = {
    runResults match {
      case r: RunResults.Successful => {
        SbatchJobSubmitter.extractJobId(r.stdout) match {
          case Some(jobId) =>  makeSuccess(jobId, taskArray)
          case None => {
            logAndMakeFailure(r) { r =>
              s"SLURM Job submission failure: couldn't determine job ID from output of `${r.commandLine}`: ${r.stdout}"
            }
          }
        }
      } 
      case r: RunResults.Unsuccessful => {
        logAndMakeFailure(r) { r =>
          s"SLURM Job submission failure: `${r.commandLine}` failed with status code ${r.exitCode}"
        }
      }
      case r: RunResults.CouldNotStart => {
        logAndMakeFailure(r) { r =>
          val msg = s"SLURM Job submission failure: `${r.commandLine}` couldn't start: ${r.cause.getMessage}"
          
          error(msg, r.cause)
          
          msg
        }
      }
    }
  }
  
  //TODO: Factor out
  private def makeSuccess(jobId: String, taskArray: DrmTaskArray): DrmSubmissionResult.SubmissionSuccess = {
    import Traversables.Implicits._
          
    def drmTaskId(drmJob: DrmJobWrapper): DrmTaskId = DrmTaskId(jobId, drmJob.drmIndex)
    
    val drmTaskIdsToJobs: Map[DrmTaskId, DrmJobWrapper] = taskArray.drmJobs.mapBy(drmTaskId)

    debug {
      val numJobs = taskArray.size
      val allJobIds = drmTaskIdsToJobs.keys
      
      s"Successfully submitted ${numJobs} SLURM jobs with base job id '${jobId}'; individual job ids: ${allJobIds}"
    }
    
    DrmSubmissionResult.SubmissionSuccess(drmTaskIdsToJobs)
  }
  
  private def logAndMakeFailure[R <: RunResults](
      runResults: R)(errorMsg: R => String): DrmSubmissionResult.SubmissionFailure = {
    
    runResults.logStdOutAndStdErr("LSF Job submission failure, stdout and stderr follow:")
      
    failure(errorMsg(runResults))
  }
}

object SbatchJobSubmitter extends Loggable {
  type Params = (DrmSettings, DrmTaskArray)
  
  type SubmissionFn = CommandInvoker.InvocationFn[Params]
  
  def fromExecutable(
      lsfConfig: SlurmConfig, 
      actualExecutable: String = "sbatch")(implicit scheduler: Scheduler): SbatchJobSubmitter = {
    
    val invoker = new CommandInvoker.Async.JustOnce[Params](
        binaryName = actualExecutable, 
        delegateFn = invokeBinaryToSubmitJobs(lsfConfig, actualExecutable))
    
    new SbatchJobSubmitter(invoker)
  }
  
  private[slurm] def invokeBinaryToSubmitJobs(
      lsfConfig: SlurmConfig, 
      actualExecutable: String): SubmissionFn = { case (drmSettings, taskArray) =>
        
    val tokens = makeTokens(actualExecutable, lsfConfig, taskArray, drmSettings)
    
    debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
    
    import scala.sys.process._
    
    val processBuilder: ProcessBuilder = tokens
    
    Processes.runSync(tokens)(processBuilder = processBuilder)
  }
  
  import DrmSubmissionResult.SubmissionFailure
  
  private[slurm] def failure(msg: String): SubmissionFailure = SubmissionFailure(new Exception(msg))
  
  private[slurm] def makeTokens(
      actualExecutable: String, 
      lsfConfig: SlurmConfig, 
      taskArray: DrmTaskArray,
      drmSettings: DrmSettings): Seq[String] = {
    
    //See https://slurm.schedmd.com/sbatch.html
    //    https://slurm.schedmd.com/heterogeneous_jobs.html

    val drmScriptFile = taskArray.drmScriptFile
    
    val runTimeInHours: Int = drmSettings.maxRunTime.hours.toInt
    val maxRunTimePart = Seq("-t", s"${runTimeInHours}:0:0")
    
    val memoryPerCoreInGigs = drmSettings.memoryPerCore.gb.toLong
    
    val memoryPart = Seq(s"--mem-per-cpu=${memoryPerCoreInGigs}G")
    
    val numCores = drmSettings.cores.value
    
    val arrayIndicesPart: Seq[String] = Seq(s"--array=0-${taskArray.size}") 
    
    val coresPart = Seq(s"--cpus-per-task=${numCores}")
    
    val queuePart: Seq[String] = Nil //TODO does Slurm have the notion of a queue
    
    val jobNamePart = Seq("-J", s"${taskArray.drmJobName}[1-${taskArray.size}]")
    
    val stdoutPart = Seq("-o", s"${taskArray.stdOutPathTemplate}")
    
    val stderrPart = Seq("-e", s"${taskArray.stdErrPathTemplate}")
    
    val tokens = actualExecutable +: 
      (arrayIndicesPart ++ queuePart ++ maxRunTimePart ++ memoryPart ++ 
       coresPart ++ jobNamePart ++ stdoutPart ++ stderrPart)
      
    tokens
  }
  
  private val submittedJobIdRegex = """^Job\s+<(\d+)>.+$""".r
  
  private[slurm] def extractJobId(stdOutLines: Seq[String]): Option[String] = {
    trace(s"Parsing job-submission output: $stdOutLines")
    
    stdOutLines.iterator.map(_.trim).filter(_.nonEmpty).collectFirst {
      case submittedJobIdRegex(jobId) => jobId
    }
  }
}