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
import loamstream.util.CommandInvoker
import rx.lang.scala.schedulers.IOScheduler
import loamstream.util.Observables
import loamstream.drm.DrmTaskId
import loamstream.util.Options
import loamstream.util.Tries
import scala.concurrent.ExecutionContext
import loamstream.drm.SessionSource


/**
 * @author clint
 * Jul 14, 2020
 */
final class QsubJobSubmitter private[uger] (
    commandInvoker: CommandInvoker[Qsub.Params],
    ugerConfig: UgerConfig)(implicit ec: ExecutionContext) extends JobSubmitter with Loggable {
  
  override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): Observable[DrmSubmissionResult] = {
    val resultsObs = Observable.from(commandInvoker.apply(Qsub.Params(ugerConfig, drmSettings, taskArray)))
    
    import QsubJobSubmitter.parseQsubOutput
    
    val taskIdsObs = resultsObs.flatMap(results => Observable.just(parseQsubOutput(results.stdout)))
    
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
      sessionSource: SessionSource, 
      ugerConfig: UgerConfig, 
      actualExecutable: String = "qsub"): QsubJobSubmitter = {
    
    //TODO
    val scheduler = IOScheduler()
    import scala.concurrent.ExecutionContext.Implicits._
    
    new QsubJobSubmitter(
        Qsub.commandInvoker(sessionSource, ugerConfig, actualExecutable, scheduler), 
        ugerConfig)
  }
  
  private object Regexes {
    //Your job-array 19115592.1-2:1 ("test.sh") has been submitted
    val jobNameIndexRangeAndStep = """^Your\s+job-array\s+(.+?)\.(\d+)-(\d+)\:(\d+).+?has been submitted$""".r
  }
  
  private[uger] def parseQsubOutput(lines: Seq[String]): Try[Seq[DrmTaskId]] = {
    val resultOpt = lines.iterator.map(_.trim).collectFirst {
      case Regexes.jobNameIndexRangeAndStep(jobId, startString, endString, stepString) => {
        Try {
          val indexRange = (startString.toInt to endString.toInt by stepString.toInt)
          
          indexRange.map(i => DrmTaskId(jobId, i))
        }
      }
    }
    
    resultOpt.getOrElse(Tries.failure(s"Failed to parse qsub output: $lines"))
  }
}
