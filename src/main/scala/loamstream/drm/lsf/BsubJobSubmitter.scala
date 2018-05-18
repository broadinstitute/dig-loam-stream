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

/**
 * @author clint
 * May 15, 2018
 */
final class BsubJobSubmitter private[lsf] (
    submissionFn: BsubJobSubmitter.SubmissionFn) extends JobSubmitter with Loggable {
    
  import BsubJobSubmitter._
  
  override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): DrmSubmissionResult = {
    //bsub -W 1:0 -R "rusage[mem=1024]" -J "helloworld[1-3]" -o :logs/out.%J.%I -e :logs/err.%J.%I < ./arr.sh
    
    val runAttempt = submissionFn(drmSettings, taskArray)
    
    runAttempt.map(toDrmSubmissionResult(taskArray)) match {
      case Success(submissionResult) => submissionResult
      case Failure(e: Exception) => DrmSubmissionResult.SubmissionFailure(e)
      case Failure(e) => DrmSubmissionResult.SubmissionFailure(new Exception(e))
    }
  }
  
  override def stop(): Unit = ()
  
  private def logStdOutAndStdErr(runResults: RunResults, headerMessage: String): Unit = {
    error(headerMessage)
    runResults.stderr.foreach(line => error(s"${runResults.executable} <via stderr>: $line"))
    runResults.stdout.foreach(line => error(s"${runResults.executable} <via stdout>: $line"))
  }
    
  private[lsf] def toDrmSubmissionResult(taskArray: DrmTaskArray)(runResults: RunResults): DrmSubmissionResult = {
    if(ExitCodes.isSuccess(runResults.exitCode)) {
      BsubJobSubmitter.extractJobId(runResults.stdout) match {
        case Some(jobId) => {
          import Traversables.Implicits._
          
          def lsfJobId(drmJob: DrmJobWrapper): String = LsfJobId(jobId, drmJob.drmIndex).asString
          
          val idsToJobs: Map[String, DrmJobWrapper] = taskArray.drmJobs.mapBy(lsfJobId)
          
          DrmSubmissionResult.SubmissionSuccess(idsToJobs)
        }
        case None => {
          logStdOutAndStdErr(runResults, "LSF Job submission failure, stdout and stderr follow:")
          
          import runResults.{ stdout, executable }
          
          val msg = s"LSF Job submission failure: couldn't determine job ID from output of `${executable}`: ${stdout}"
      
          failure(msg)
        }
      }
    } else {
      logStdOutAndStdErr(runResults, "LSF Job submission failure, stdout and stderr follow:")
      
      import runResults.{ exitCode, executable }
      
      val msg = s"LSF Job submission failure: `${executable}` failed with status code ${exitCode}"
      
      failure(msg)
    }
  }
}

object BsubJobSubmitter {
  type SubmissionFn = (DrmSettings, DrmTaskArray) => Try[RunResults]
  
  def fromActualBinary(actualExecutable: String = "bsub"): BsubJobSubmitter = {
    new BsubJobSubmitter(invokeBinaryToSubmitJobs(actualExecutable))
  }
  
  private[lsf] def invokeBinaryToSubmitJobs(actualExecutable: String): SubmissionFn = { (drmSettings, taskArray) =>
    import scala.sys.process._
  
    val tokens = makeTokens(actualExecutable, taskArray, drmSettings)
    
    val processBuilder: ProcessBuilder = tokens #< taskArray.drmScriptFile.toFile
    
    Processes.runSync(actualExecutable, processBuilder)
  }
  
  private[lsf] def failure(msg: String) = DrmSubmissionResult.SubmissionFailure(new Exception(msg))
  
  private[lsf] def makeTokens(
      actualExecutable: String, 
      taskArray: DrmTaskArray,
      drmSettings: DrmSettings): Seq[String] = {
    
    val runTimeInHours: Int = drmSettings.maxRunTime.hours.toInt
    val maxRunTimePart = Seq("-W", s"${runTimeInHours}:0")
    
    val memoryPerCoreInMegs = drmSettings.memoryPerCore.mib.toInt
    val memoryPart = Seq("-R", s""""rusage[mem=${memoryPerCoreInMegs}]"""")
    
    val numCores = drmSettings.cores.value
    
    val coresPart = Seq("-n", numCores.toString, "-R", """"span[hosts=1]"""")
    
    val queuePart: Seq[String] = drmSettings.queue.toSeq.flatMap(q => Seq("-q", q.name))
    
    val jobNamePart = Seq("-J", s""""${taskArray.drmJobName}[1-${taskArray.size}]"""")
    
    val stdoutPart = Seq("-oo", s":${taskArray.stdOutPathTemplate}")
    
    val stderrPart = Seq("-eo", s":${taskArray.stdErrPathTemplate}")
    
    actualExecutable +: 
        (queuePart ++ maxRunTimePart ++ memoryPart ++ coresPart ++ jobNamePart ++ stdoutPart ++ stderrPart)
  }
  
  private val submittedJobIdRegex = """^Job\s+<(\d+)>.+$""".r
  
  private[lsf] def extractJobId(stdOutLines: Seq[String]): Option[String] = {
    stdOutLines.iterator.map(_.trim).filter(_.nonEmpty).collectFirst {
      case submittedJobIdRegex(jobId) => jobId
    }
  }
}
