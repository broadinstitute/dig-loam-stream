package loamstream.drm.uger


import scala.util.Try

import loamstream.conf.UgerConfig
import loamstream.drm.DrmSubmissionResult
import loamstream.drm.DrmTaskArray
import loamstream.drm.DrmTaskId
import loamstream.drm.JobSubmitter
import loamstream.model.execute.DrmSettings
import loamstream.util.CommandInvoker
import loamstream.util.LogContext
import loamstream.util.Loggable
import loamstream.util.RunResults
import loamstream.util.Tries
import monix.execution.Scheduler
import monix.reactive.Observable


/**
 * @author clint
 * Jul 14, 2020
 */
final class QsubJobSubmitter private[uger] (
    commandInvoker: CommandInvoker.Async[Qsub.Params],
    ugerConfig: UgerConfig) extends JobSubmitter with Loggable {
  
  override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): Observable[DrmSubmissionResult] = {
    val resultsObs = Observable.from(commandInvoker.apply(Qsub.Params(ugerConfig, drmSettings, taskArray)))
    
    import QsubJobSubmitter.parseQsubOutput
    
    val taskIdsObs = resultsObs.flatMap { results => 
      Observable(parseQsubOutput(results.stdout, taskArray.drmJobName))
    }
    
    for {
      taskIdsAttempt <- taskIdsObs
    } yield {
      taskIdsAttempt.map { taskIds =>
        taskIds.zip(taskArray.drmJobs).toMap
      }
    }
  }
  
  override def stop(): Unit = ()
}

object QsubJobSubmitter extends Loggable {
  
  type SubmissionFn = (DrmSettings, DrmTaskArray) => Try[RunResults]
  
  def fromExecutable(
      ugerConfig: UgerConfig, 
      actualExecutable: String = "qsub")(
      implicit scheduler: Scheduler): QsubJobSubmitter = {
    
    new QsubJobSubmitter(
        Qsub.commandInvoker(ugerConfig, actualExecutable, scheduler), 
        ugerConfig)
  }
  
  private object Regexes {
    //Your job-array 19115592.1-2:1 ("test.sh") has been submitted
    val jobNameIndexRangeAndStep = """^Your\s+job-array\s+(.+?)\.(\d+)-(\d+)\:(\d+).+?has been submitted$""".r
  }
  
  private[uger] def parseQsubOutput(
      lines: Seq[String], 
      taskArrayName: String)(implicit ctx: LogContext): Try[Seq[DrmTaskId]] = {
    
    val resultOpt = lines.iterator.map(_.trim).collectFirst {
      case Regexes.jobNameIndexRangeAndStep(jobId, startString, endString, stepString) => {
        
        ctx.debug(s"Submitted task array with LS name '${taskArrayName}' as Uger job id ${jobId}")
        
        Try {
          val indexRange = (startString.toInt to endString.toInt by stepString.toInt)
          
          indexRange.map(i => DrmTaskId(jobId, i))
        }
      }
    }
    
    resultOpt.getOrElse(Tries.failure(s"Failed to parse qsub output: $lines"))
  }
}
