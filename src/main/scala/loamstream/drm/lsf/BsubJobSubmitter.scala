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

/**
 * @author clint
 * May 15, 2018
 */
final class BsubJobSubmitter(
    drmConfig: DrmConfig, 
    actualExecutable: String = "bsub") extends JobSubmitter with Loggable {
    
  override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): DrmSubmissionResult = {
    //bsub -W 1:0 -R "rusage[mem=1024]" -J "helloworld[1-3]" -o :logs/out.%J.%I -e :logs/err.%J.%I < ./arr.sh
    
    import scala.sys.process._
    import BsubJobSubmitter._
    
    val tokens = makeTokens(actualExecutable, taskArray, drmSettings)
    
    val processBuilder: ProcessBuilder = tokens #< taskArray.drmScriptFile.toFile
    
    val runAttempt = run(processBuilder)
    
    def failure(msg: String) = DrmSubmissionResult.SubmissionFailure(new Exception(msg))
    
    def logStdOutAndStdErr(runResults: RunResults, headerMessage: String): Unit = {
      error(headerMessage)
      runResults.stderr.foreach(line => error(s"${actualExecutable} <via stderr>: $line"))
      runResults.stdout.foreach(line => error(s"${actualExecutable} <via stdout>: $line"))
    }
    
    def toDrmSubmissionResult(runResults: RunResults): DrmSubmissionResult = {
      if(ExitCodes.isSuccess(runResults.exitCode)) {
        BsubJobSubmitter.extractJobId(runResults.stdout) match {
          case Some(jobId) => {
            val individualJobIds = (1 to taskArray.size).map(i => s"${jobId}[${i}]")
            
            val idsToJobs = individualJobIds.zip(taskArray.drmJobs).toMap
            
            DrmSubmissionResult.SubmissionSuccess(idsToJobs)
          }
          case None => {
            logStdOutAndStdErr(runResults, "LSF Job submission failure, stdout and stderr follow:")
            
            val msg = s"LSF Job submission failure: couldn't determine job ID from output of `${actualExecutable}`: ${runResults.stdout}"
        
            failure(msg)
          }
        }
      } else {
        logStdOutAndStdErr(runResults, "LSF Job submission failure, stdout and stderr follow:")
        
        val msg = s"LSF Job submission failure: `${actualExecutable}` failed with status code ${runResults.exitCode}"
        
        failure(msg)
      }
    }
    
    runAttempt.map(toDrmSubmissionResult) match {
      case Success(submissionResult) => submissionResult
      case Failure(e: Exception) => DrmSubmissionResult.SubmissionFailure(e)
      case Failure(e) => DrmSubmissionResult.SubmissionFailure(new Exception(e))
    }
  }
  
  override def stop(): Unit = ()
}

object BsubJobSubmitter {
  private[lsf] final case class RunResults(exitCode: Int, stdout: Seq[String], stderr: Seq[String])
  
  import scala.sys.process._
  
  private[lsf] def run(processBuilder: ProcessBuilder): Try[RunResults] = {
    val stdOutBuffer: Buffer[String] = new ArrayBuffer
    val stdErrBuffer: Buffer[String] = new ArrayBuffer
    
    val processLogger = ProcessLogger(stdOutBuffer += _, stdErrBuffer += _)
    
    Try {
      val exitCode = processBuilder.!(processLogger)
    
      RunResults(exitCode, stdOutBuffer.toList, stdErrBuffer.toList)
    }
  }
  
  private[lsf] def makeTokens(
      actualExecutable: String, 
      taskArray: DrmTaskArray,
      drmSettings: DrmSettings): Seq[String] = {
    
    val runTimeInHours: Int = drmSettings.maxRunTime.hours.toInt
    val maxRunTimePart = Seq("-W", s"${runTimeInHours}:0")
    
    val memoryPerCoreInMegs = drmSettings.memoryPerCore.mb.toInt
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
  
  private val submittedJobIdRegex = """^Job <(\d+)> is submitted""".r
  
  private[lsf] def extractJobId(stdOutLines: Seq[String]): Option[String] = {
    stdOutLines.iterator.map(_.trim).collectFirst {
      case submittedJobIdRegex(jobId) => jobId
    }
  }
}
