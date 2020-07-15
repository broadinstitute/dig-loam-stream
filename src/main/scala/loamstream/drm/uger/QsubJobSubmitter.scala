package loamstream.drm.uger

import loamstream.drm.JobSubmitter
import rx.lang.scala.Observable
import loamstream.model.execute.DrmSettings
import loamstream.drm.DrmTaskArray
import loamstream.drm.DrmSubmissionResult
import loamstream.util.Loggable
import loamstream.conf.UgerConfig
import loamstream.util.Processes
import scala.util.Try
import loamstream.util.RunResults
import loamstream.util.Users
import java.util.UUID


/**
 * @author clint
 * Jul 14, 2020
 */
final class QsubJobSubmitter private[uger] (
    submissionFn: QsubJobSubmitter.SubmissionFn) extends JobSubmitter with Loggable {
  
  override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): Observable[DrmSubmissionResult] = {
    ???
  }
  
  override def stop(): Unit = ()
}

object QsubJobSubmitter extends Loggable {
  
  type SubmissionFn = (DrmSettings, DrmTaskArray) => Try[RunResults]
  
  def fromExecutable(ugerConfig: UgerConfig, actualExecutable: String = "qsub"): QsubJobSubmitter = {
    new QsubJobSubmitter(invokeBinaryToSubmitJobs(ugerConfig, actualExecutable))
  }
  
  private[uger] def invokeBinaryToSubmitJobs(
      ugerConfig: UgerConfig, 
      actualExecutable: String): SubmissionFn = { (drmSettings, taskArray) =>
        
    val tokens = makeTokens(actualExecutable, ugerConfig, taskArray, drmSettings)
    
    debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
    
    import scala.sys.process._
    
    //NB: script contents are piped to bsub
    val processBuilder: ProcessBuilder = tokens #< taskArray.drmScriptFile.toFile
    
    Processes.runSync(actualExecutable, processBuilder)
  }
  
  private[uger] def makeTokens(
      actualExecutable: String, 
      lsfConfig: UgerConfig, 
      taskArray: DrmTaskArray,
      drmSettings: DrmSettings): Seq[String] = {
    
    ???
    /*
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
    
    val queuePart: Seq[String] = drmSettings.queue.toSeq.flatMap(q => Seq("-q", q.name))
    
    val jobNamePart = Seq("-J", s"${taskArray.drmJobName}[1-${taskArray.size}]")
    
    val stdoutPart = Seq("-oo", s"${taskArray.stdOutPathTemplate}")
    
    val stderrPart = Seq("-eo", s"${taskArray.stdErrPathTemplate}")
    
    val tokens = actualExecutable +: 
      (queuePart ++ maxRunTimePart ++ memoryDashMPart ++ memoryRusagePart ++ 
       coresPart ++ jobNamePart ++ stdoutPart ++ stderrPart)
      
    tokens*/
  }
}
