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
import loamstream.drm.DockerParams
import loamstream.conf.LsfConfig

/**
 * @author clint
 * May 15, 2018
 */
final class BsubJobSubmitter private[lsf] (
    submissionFn: BsubJobSubmitter.SubmissionFn) extends JobSubmitter with Loggable {
    
  import BsubJobSubmitter._
  
  override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): DrmSubmissionResult = {
    val runAttempt = submissionFn(drmSettings, taskArray)
    
    runAttempt.map(toDrmSubmissionResult(taskArray)) match {
      case Success(submissionResult) => submissionResult
      case Failure(e: Exception) => DrmSubmissionResult.SubmissionFailure(e)
      case Failure(e) => DrmSubmissionResult.SubmissionFailure(new Exception(e))
    }
  }
  
  override def stop(): Unit = ()
  
  private[lsf] def toDrmSubmissionResult(taskArray: DrmTaskArray)(runResults: RunResults): DrmSubmissionResult = {
    if(runResults.isSuccess) {
      BsubJobSubmitter.extractJobId(runResults.stdout) match {
        case Some(jobId) =>  makeSuccess(jobId, taskArray)
        case None => {
          logAndMakeFailure(runResults) { r =>
            s"LSF Job submission failure: couldn't determine job ID from output of `${r.executable}`: ${r.stdout}"
          }
        }
      }
    } else {
      logAndMakeFailure(runResults) { runResults =>
        s"LSF Job submission failure: `${runResults.executable}` failed with status code ${runResults.exitCode}"
      }
    }
  }
  
  private def makeSuccess(jobId: String, taskArray: DrmTaskArray): DrmSubmissionResult.SubmissionSuccess = {
    import Traversables.Implicits._
          
    def lsfJobId(drmJob: DrmJobWrapper): String = LsfJobId(jobId, drmJob.drmIndex).asString
    
    val idsToJobs: Map[String, DrmJobWrapper] = taskArray.drmJobs.mapBy(lsfJobId)

    debug {
      val numJobs = taskArray.size
      val allJobIds = idsToJobs.keys
      
      s"Successfully submitted ${numJobs} LSF jobs with base job id '${jobId}'; individual job ids: ${allJobIds}"
    }
    
    DrmSubmissionResult.SubmissionSuccess(idsToJobs)
  }
  
  private def logAndMakeFailure(
      runResults: RunResults)(errorMsg: RunResults => String): DrmSubmissionResult.SubmissionFailure = {
    
    runResults.logStdOutAndStdErr("LSF Job submission failure, stdout and stderr follow:")
      
    failure(errorMsg(runResults))
  }
}

object BsubJobSubmitter extends Loggable {
  type SubmissionFn = (DrmSettings, DrmTaskArray) => Try[RunResults]
  
  def fromExecutable(lsfConfig: LsfConfig, actualExecutable: String = "bsub"): BsubJobSubmitter = {
    new BsubJobSubmitter(invokeBinaryToSubmitJobs(lsfConfig, actualExecutable))
  }
  
  private[lsf] def invokeBinaryToSubmitJobs(
      lsfConfig: LsfConfig, 
      actualExecutable: String): SubmissionFn = { (drmSettings, taskArray) =>
        
    import scala.sys.process._
  
    val tokens = makeTokens(actualExecutable, lsfConfig, taskArray, drmSettings)
    
    debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
    
    val processBuilder: ProcessBuilder = tokens #< taskArray.drmScriptFile.toFile
    
    Processes.runSync(actualExecutable, processBuilder)
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
    
    val memoryPerCoreInMegs = drmSettings.memoryPerCore.mb.toInt
    val memoryPart = Seq("-R", s"rusage[mem=${memoryPerCoreInMegs}]")
    
    val numCores = drmSettings.cores.value
    
    val coresPart = Seq("-n", numCores.toString, "-R", s"span[hosts=1]")
    
    val queuePart: Seq[String] = drmSettings.queue.toSeq.flatMap(q => Seq("-q", q.name))
    
    val jobNamePart = Seq("-J", s"${taskArray.drmJobName}[1-${taskArray.size}]")
    
    val stdoutPart = Seq("-oo", s"${taskArray.stdOutPathTemplate}")
    
    val stderrPart = Seq("-eo", s"${taskArray.stdErrPathTemplate}")
    
    val tokens = actualExecutable +: 
      (queuePart ++ maxRunTimePart ++ memoryPart ++ coresPart ++ jobNamePart ++ stdoutPart ++ stderrPart)
      
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
