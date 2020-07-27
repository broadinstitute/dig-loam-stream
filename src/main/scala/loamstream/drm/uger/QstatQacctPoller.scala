package loamstream.drm.uger

import scala.concurrent.ExecutionContext
import scala.util.Success
import scala.util.Try

import loamstream.drm.DrmStatus
import loamstream.drm.DrmTaskId
import loamstream.drm.Poller
import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.Options
import loamstream.util.Tries
import rx.lang.scala.Observable
import rx.lang.scala.schedulers.IOScheduler
import loamstream.drm.SessionSource
import loamstream.conf.UgerConfig
import scala.util.control.NonFatal

/**
 * @author clint
 * Jul 15, 2020
 */
final class QstatQacctPoller private[uger] (
    qstatInvoker: CommandInvoker[Unit],
    qacctInvoker: CommandInvoker[DrmTaskId]) extends Poller with Loggable {
  
  import QstatQacctPoller._
  
  override def poll(jobIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, Try[DrmStatus])] = {
    //TODO: Invoke qstat, get info for all jobs, parse output, run qacct for any missing jobs
    /*
     * job-ID     prior   name       user         state submit/start at     queue                          jclass                         slots ja-task-ID
     * ------------------------------------------------------------------------------------------------------------------------------------------------
     * 19115592 0.56956 test.sh    cgilbert     r     07/24/2020 11:51:17 broad@uger-c104.broadinstitute                                    1 1
     * 19115592 0.56956 test.sh    cgilbert     r     07/24/2020 11:51:18 broad@uger-c104.broadinstitute                                    1 2
     *         
     */
    //TODO
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val qstatResultObs = Observable.from(qstatInvoker.apply(()))

    val jobIdSet = jobIds.toSet
    
    val pollingResultsFromQstatObs = qstatResultObs.map { qstatResults => 
      QstatSupport.getByTaskId(qstatResults.stdout)
    }
    
    pollingResultsFromQstatObs.flatMap { byTaskId =>
      val notFoundByQstat = jobIdSet -- byTaskId.keys
      
      //TODO: chunk up not-found task ids, to avoid running qacct too many times?
      
      val qacctResultsObs = Observables.merge(notFoundByQstat.iterator.map(invokeQacctFor).toIterable)
      
      Observable.from(byTaskId) ++ qacctResultsObs.map { case (tid, status) => (tid, Success(status)) }
    }
  }
  
  private def invokeQacctFor(drmTaskId: DrmTaskId)(implicit ec: ExecutionContext): Observable[(DrmTaskId, DrmStatus)] = {
    val f = qacctInvoker(drmTaskId).map(drmTaskId -> _.stdout).map(QacctSupport.parseQacctResults).recover {
      case NonFatal(e) => drmTaskId -> DrmStatus.Undetermined 
    }
    
    Observable.from(f)
  }
  
  override def stop(): Unit = ()
}

object QstatQacctPoller extends Loggable {
    
  def fromExecutables(
      sessionSource: SessionSource,
      actualQstatExecutable: String = "qstat",
      actualQacctExecutable: String = "qacct")(implicit ec: ExecutionContext): QstatQacctPoller = {
    
    import QacctInvoker.{ useActualBinary => qacctCommandInvoker }
    import Qstat.{ commandInvoker => qstatCommandInvoker }
    
    val qacct = qacctCommandInvoker(0, "qacct", IOScheduler())
    val qstat = qstatCommandInvoker(sessionSource, actualQstatExecutable)
    
    new QstatQacctPoller(qstat, qacct)
  }
  
  private[uger] object QstatSupport {
    object QstatRegexes {
      val jobIdStatusAndTaskIndex = """^(\w+)\s+.+?\s+.+?\s+.+?\s+(\w+)\s+.+?(\d+)$""".r
    }
    
    def getByTaskId(qstatOutput: Seq[String]): Map[DrmTaskId, Try[DrmStatus]] = {
      parseQstatOutput(qstatOutput).collect { 
        case Success((drmTaskId, drmStatus)) => drmTaskId -> Success(drmStatus) 
      }.toMap 
    }
    
    def parseQstatOutput(lines: Seq[String]): Seq[Try[(DrmTaskId, DrmStatus)]] = {
      lines.iterator.map(_.trim).collect {
        case QstatRegexes.jobIdStatusAndTaskIndex(jobId, ugerStatusCode, taskIndexString) => {
          for {
            drmTaskId <- Try(DrmTaskId(jobId, taskIndexString.toInt))
            drmStatus <- toDrmStatus(ugerStatusCode.trim)
          } yield drmTaskId -> drmStatus
        }
      }.toList
    }
    
    def toDrmStatus(ugerStatusCode: String): Try[DrmStatus] = {
      val parseMappedCodes: PartialFunction[String, DrmStatus] = {
        case "E" => DrmStatus.Failed //TODO ???
        case "h" => DrmStatus.QueuedHeld //TODO: ???
        case "r" | "R" => DrmStatus.Running
        case "s" | "S" | "N" => DrmStatus.Suspended
        case "w" | "qw" => DrmStatus.Queued //TODO: ???
        case u @ ("d" | "P" | "t" | "T") => {
          warn(s"Unmapped Uger status code '${u}' mapped to '${DrmStatus.Undetermined}'")
          
          DrmStatus.Undetermined
        }
      }
      
      Success(ugerStatusCode).collect(parseMappedCodes) match {
        case s @ Success(_) => s
        case failure => Tries.failure(s"Unknown Uger status code '$ugerStatusCode' encountered")
      }
      
      /*
       * the  status  of  the  job  - one of:
       *   d(eletion), 
       *   E(rror), 
       *   h(old), 
       *   r(unning), 
       *   R(estarted), 
       *   s(uspended), 
       *   S(uspended), 
       *   e(N)hanced suspended, 
       *   (P)reempted, 
       *   t(ransfering), 
       *   T(hreshold) or
       *   w(aiting).
       */
      /*ugerStatusCode match {
        case "E" => Success(DrmStatus.Failed) //TODO ???
        case "h" => Success(DrmStatus.QueuedHeld) //TODO: ???
        case "r" | "R" => Success(DrmStatus.Running)
        case "s" | "S" | "N" => Success(DrmStatus.Suspended)
        case "w" => Success(DrmStatus.Queued) //TODO: ???
        case u @ ("d" | "P" | "t" | "T") => {
          warn(s"Unmapped Uger status code '${u}' mapped to '${DrmStatus.Undetermined}'")
          
          Success(DrmStatus.Undetermined)
        }
        case _ => Tries.failure(s"Unknown Uger status code '$ugerStatusCode' encountered")
      }*/
    }
  }
  
  private[uger] object QacctSupport {
    object Regexes {
      val exitStatus = "exit_status\\s+(.+?)$".r
    }
    
    def parseQacctResults(t: (DrmTaskId, Seq[String])): (DrmTaskId, DrmStatus) = {
      import loamstream.util.Tuples.Implicits.Tuple2Ops
        
      val (tid, _) = t
      
      def parseOutputLines(lines: Seq[String]): DrmStatus = {
        getExitStatus(lines).map(DrmStatus.fromExitCode).getOrElse(DrmStatus.Undetermined)
      }
      
      t.mapSecond(parseOutputLines)
    }
    
    def getExitStatus(qacctOutput: Seq[String]): Try[Int] = {
      val opt = qacctOutput.iterator.map(_.trim).collectFirst { 
        case Regexes.exitStatus(value) => value.trim 
      }.filter(_.nonEmpty)
          
      Options.toTry(opt)(s"Couldn't find 'exit_status' field").map(_.toInt)
    }
  }
}
