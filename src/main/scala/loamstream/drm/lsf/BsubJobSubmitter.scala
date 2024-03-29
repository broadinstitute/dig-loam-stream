package loamstream.drm.lsf

import loamstream.conf.DrmConfig
import loamstream.drm.JobSubmitter
import loamstream.util.Loggable
import loamstream.model.execute.DrmSettings
import loamstream.drm.DrmTaskArray
import loamstream.drm.DrmSubmissionResult
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import loamstream.util.ExitCodes
import loamstream.drm.DrmJobWrapper
import loamstream.util.Traversables
import scala.collection.mutable.ListBuffer
import java.nio.file.Path
import loamstream.drm.ContainerParams
import loamstream.conf.LsfConfig
import loamstream.util.RunResults
import loamstream.util.Processes
import loamstream.drm.DrmTaskId
import monix.reactive.Observable
import scala.collection.compat._
import loamstream.util.CommandInvoker

/**
 * @author clint
 * May 15, 2018
 */
final class BsubJobSubmitter private[lsf] (
    submissionFn: CommandInvoker.Sync[BsubJobSubmitter.Params]) extends JobSubmitter with Loggable {
    
  import BsubJobSubmitter._
  
  override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): Observable[DrmSubmissionResult] = {
    val runAttemptObs = Observable(submissionFn(drmSettings, taskArray))
    
    runAttemptObs.map { attempt =>
      attempt.map(toDrmSubmissionResult(taskArray)) match {
        case Success(submissionResult) => submissionResult
        case Failure(e) => DrmSubmissionResult.SubmissionFailure(e)
      }
    }
  }
  
  override def stop(): Unit = ()
  
  private[lsf] def toDrmSubmissionResult(taskArray: DrmTaskArray)(runResults: RunResults): DrmSubmissionResult = {
    runResults match {
      case r @ RunResults.Completed(_, exitCode, _, _) if ExitCodes.isSuccess(exitCode) => {
        BsubJobSubmitter.extractJobId(r.stdout) match {
          case Some(jobId) =>  makeSuccess(jobId, taskArray)
          case None => {
            logAndMakeFailure(r) { r =>
              s"LSF Job submission failure: couldn't determine job ID from output of `${r.commandLine}`: ${r.stdout}"
            }
          }
        }
      } 
      case r: RunResults.Completed => {
        logAndMakeFailure(r) { r =>
          s"LSF Job submission failure: `${r.commandLine}` failed with status code ${r.exitCode}"
        }
      }
      case r: RunResults.CouldNotStart => {
        logAndMakeFailure(r) { r =>
          val msg = s"LSF Job submission failure: `${r.commandLine}` couldn't start: ${r.cause.getMessage}"
          
          error(msg, r.cause)
          
          msg
        }
      }
    }
  }
  
  private def makeSuccess(jobId: String, taskArray: DrmTaskArray): DrmSubmissionResult.SubmissionSuccess = {
    import Traversables.Implicits._
          
    def drmTaskId(drmJob: DrmJobWrapper): DrmTaskId = DrmTaskId(jobId, drmJob.drmIndex)
    
    val drmTaskIdsToJobs: Map[DrmTaskId, DrmJobWrapper] = taskArray.drmJobs.mapBy(drmTaskId)

    debug {
      val numJobs = taskArray.size
      val allJobIds = drmTaskIdsToJobs.keys
      
      s"Successfully submitted ${numJobs} LSF jobs with base job id '${jobId}'; individual job ids: ${allJobIds}"
    }
    
    DrmSubmissionResult.SubmissionSuccess(drmTaskIdsToJobs)
  }
  
  private def logAndMakeFailure[R <: RunResults](
      runResults: R)(errorMsg: R => String): DrmSubmissionResult.SubmissionFailure = {
    
    runResults.logStdOutAndStdErr("LSF Job submission failure, stdout and stderr follow:")
      
    failure(errorMsg(runResults))
  }
}

object BsubJobSubmitter extends Loggable {
  type Params = (DrmSettings, DrmTaskArray)
  
  type SubmissionFn = CommandInvoker.InvocationFn[Params]
  
  def fromExecutable(lsfConfig: LsfConfig, actualExecutable: String = "bsub"): BsubJobSubmitter = {
    val invoker = new CommandInvoker.Sync.JustOnce[Params](
        binaryName = actualExecutable, 
        delegateFn = invokeBinaryToSubmitJobs(lsfConfig, actualExecutable),
        isSuccess = RunResults.SuccessPredicate.ByExitCode.zeroIsSuccess)
    
    new BsubJobSubmitter(invoker)
  }
  
  private[lsf] def invokeBinaryToSubmitJobs(
      lsfConfig: LsfConfig, 
      actualExecutable: String): SubmissionFn = { 
        case (drmSettings, taskArray) => Try {
          val tokens = makeTokens(actualExecutable, lsfConfig, taskArray, drmSettings)
          
          debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
          
          import scala.sys.process._
          
          val scriptFile = taskArray.drmScriptFile.toFile
          
          //NB: script contents are piped to bsub
          val processBuilder: ProcessBuilder = tokens #< scriptFile
          
          val tokensForLogging = tokens ++ Seq("<", scriptFile.toString)
          
          Processes.runSync(tokensForLogging)(processBuilder = processBuilder)
        }
  }
  
  import DrmSubmissionResult.SubmissionFailure
  
  private[lsf] def failure(msg: String): SubmissionFailure = SubmissionFailure(new Exception(msg))
  
  private[lsf] def makeTokens(
      actualExecutable: String, 
      lsfConfig: LsfConfig, 
      taskArray: DrmTaskArray,
      drmSettings: DrmSettings): Seq[String] = {
    
    //NB: See https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.2/lsf_command_ref/bsub.1.html
    
    val runTimeInHours: Int = drmSettings.maxRunTime.hours.toInt
    val maxRunTimePart = Seq("-W", s"${runTimeInHours}:0")
    
    val memoryPerCoreInMegs = drmSettings.memoryPerCore.mb.toLong
    
    //Per the LSF docs, memory specified with -R needs to be in megs, with -M it needs to be in kb,
    //but at EBI, it needs to be in megs for both.
    val memoryRusagePart = Seq("-R", s"rusage[mem=${memoryPerCoreInMegs}]")
    val memoryDashMPart = Seq("-M", memoryPerCoreInMegs.toString)
    
    val numCores = drmSettings.cores.value
    
    val coresPart = Seq("-n", numCores.toString, "-R", s"span[hosts=1]")
    
    val queuePart: Seq[String] = drmSettings.queue.to(Seq).flatMap(q => Seq("-q", q.name))
    
    val jobNamePart = Seq("-J", s"${taskArray.drmJobName}[1-${taskArray.size}]")
    
    val stdoutPart = Seq("-oo", s"${taskArray.stdOutPathTemplate}")
    
    val stderrPart = Seq("-eo", s"${taskArray.stdErrPathTemplate}")
    
    val tokens = actualExecutable +: 
      (queuePart ++ maxRunTimePart ++ memoryDashMPart ++ memoryRusagePart ++ 
       coresPart ++ jobNamePart ++ stdoutPart ++ stderrPart)
      
    tokens
  }
  
  private val submittedJobIdRegex = """^Job\s+<(\d+)>.+$""".r
  
  private[lsf] def extractJobId(stdOutLines: Seq[String]): Option[String] = {
    trace(s"Parsing job-submission output: $stdOutLines")
    
    stdOutLines.iterator.map(_.trim).filter(_.nonEmpty).collectFirst {
      case submittedJobIdRegex(jobId) => jobId
    }
  }
}
