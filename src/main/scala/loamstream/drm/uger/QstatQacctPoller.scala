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
import loamstream.util.Traversables
import scala.util.matching.Regex
import scala.util.Failure
import scala.annotation.tailrec
import loamstream.util.Fold
import rx.lang.scala.Scheduler
import rx.lang.scala.Scheduler
import loamstream.util.Terminable
import loamstream.util.RxSchedulers
import rx.lang.scala.schedulers.ExecutionContextScheduler
import loamstream.util.Throwables
import loamstream.util.ThisMachine

/**
 * @author clint
 * Jul 15, 2020
 */
final class QstatQacctPoller private[uger] (
    qstatInvoker: CommandInvoker.Async[Unit],
    qacctInvoker: CommandInvoker.Async[String]) extends Poller with Loggable {
  
  import QstatQacctPoller._
  
  
  override def poll(drmTaskIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, Try[DrmStatus])] = {
    //Invoke qstat, to get the status of all submitted-but-not-finished jobs in this session
    val qstatResultObs = {
      implicit val ec = ExecutionContexts.forQstat
      
      Observable.from(qstatInvoker.apply(())).observeOn(Schedulers.forQstat)
    }

    //Parse out DrmTaskIds and DrmStatuses from raw qstat output (one line per task)
    val pollingResultsFromQstatObs = qstatResultObs.map { qstatResults => 
      QstatSupport.getByTaskId(qstatResults.stdout)
    }.onBackpressureDrop
    
    //The Set of distinct DrmTaskIds (jobId/task index coords) that we're polling for
    val drmTaskIdSet = drmTaskIds.toSet
    
    //For all the jobs that we're polling for that were not mentioned by qstat, assume they've finished 
    //(qstat only returns info about running jobs) and invoke qacct to determine their final status.
    pollingResultsFromQstatObs.flatMap { byTaskId =>
      val notFoundByQstat = drmTaskIdSet -- byTaskId.keys
      
      //For all the DrmTaskIds we're looking for but that weren't mentioned by qstat, 
      //determine the set of distinct job ids. Or, the ids of task arrays with finished jobs 
      //from the DrmTaskIds that we're polling for.
      val taskArrayIdsNotFoundByQstat = notFoundByQstat.map(_.jobId) 
      
      def numJobIds = taskArrayIdsNotFoundByQstat.size
      
      debug(s"${notFoundByQstat.size} finished jobs not found by qstat, ${numJobIds} job IDs")
      
      //Invoke qacct once per task-array-with-unfinished-jobs; discard info about jobs we're not polling for;
      //parse out statuses by looking at the jobs' exit codes.
      val qacctInvocationObses = taskArrayIdsNotFoundByQstat.iterator.take(ThisMachine.numCpus).map {
        implicit val ec = ExecutionContexts.forQacct
        
        invokeQacctFor(drmTaskIdSet)(_).observeOn(Schedulers.forQacct)
      }.toSeq
      
      val qacctResultsObs = Observables.merge(qacctInvocationObses)
      
      //Concatentate results from qstat with those from qacct, wrapping in Trys as needed.
      Observable.from(byTaskId) ++ {
        qacctResultsObs.map { case (tid, status) => (tid, Success(status)) }
      }.onBackpressureDrop
    }
  }
  
  private def invokeQacctFor(
      drmTaskIds: Set[DrmTaskId])
     (jobNumber: String)
     (implicit ec: ExecutionContext): Observable[(DrmTaskId, DrmStatus)] = {
    
    val tuplesObs = {
      Observable.from(qacctInvoker(jobNumber))
                .observeOn(Schedulers.forQacct)
                .map(jobNumber -> _.stdout)
                .map(QacctSupport.parseMultiTaskQacctResults(drmTaskIds))
                .flatMap(Observable.from(_))
    }
    
    tuplesObs.onErrorResumeNext {
      case NonFatal(e) => {
        warn(s"Error invoking qacct for task array with job id '${jobNumber}'")
        
        Observable.empty
      }
    }
  }
  
  override def stop(): Unit = {
    Throwables.quietly("Shutting down Qstat ExecutionContext") {
      ExecutionContexts.forQstatHandle.stop()
    }
    
    Throwables.quietly("Shutting down Qacct ExecutionContext") {
      ExecutionContexts.forQacctHandle.stop()
    }
  }
  
  private[uger] object ExecutionContexts {
    lazy val (forQstat: ExecutionContext, forQstatHandle: Terminable) = {
      loamstream.util.ExecutionContexts.singleThread
    }
    
    lazy val (forQacct: ExecutionContext, forQacctHandle: Terminable) = {
      loamstream.util.ExecutionContexts.oneThreadPerCpu
    }
  }
    
  private[uger] object Schedulers {
    val forQstat: Scheduler = ExecutionContextScheduler(ExecutionContexts.forQstat)
    
    val forQacct: Scheduler = ExecutionContextScheduler(ExecutionContexts.forQacct)
  }
}

object QstatQacctPoller extends Loggable {
    
  def fromExecutables(
      sessionSource: SessionSource,
      actualQstatExecutable: String = "qstat",
      actualQacctExecutable: String = "qacct",
      scheduler: Scheduler)(implicit ec: ExecutionContext): QstatQacctPoller = {
    
    import QacctInvoker.ByTaskArray.{ useActualBinary => qacctCommandInvoker }
    import Qstat.{ commandInvoker => qstatCommandInvoker }
    
    val qacct = qacctCommandInvoker(0, "qacct", scheduler)
    val qstat = qstatCommandInvoker(sessionSource, actualQstatExecutable)
    
    new QstatQacctPoller(qstat, qacct)
  }
  
  private[uger] object QstatSupport {
    object QstatRegexes {
      val jobIdStatusAndTaskIndex = """^(\w+)\s+.+?\s+.+?\s+.+?\s+(\w+)\s+.+?(\d+)$""".r
    }
    
    def getByTaskId(qstatOutput: Seq[String]): Map[DrmTaskId, Try[DrmStatus]] = {
      parseQstatOutput(qstatOutput).collect { 
        case Success((drmTaskId, drmStatus)) if !drmStatus.isUndetermined => drmTaskId -> Success(drmStatus) 
      }.toMap 
    }
    
    // scalastyle:off line.size.limit
    /**
     * Parse qacct output like the following.  Results are returned one line per job/task
     * 
     * job-ID     prior   name       user         state submit/start at     queue                          jclass                         slots ja-task-ID
     * ------------------------------------------------------------------------------------------------------------------------------------------------
     * 19115592 0.56956 test.sh    cgilbert     r     07/24/2020 11:51:17 broad@uger-c104.broadinstitute                                    1 1
     * 19115592 0.56956 test.sh    cgilbert     r     07/24/2020 11:51:18 broad@uger-c104.broadinstitute                                    1 2
     *         
     */
    // scalastyle:on line.size.limit
    def parseQstatOutput(lines: Seq[String]): Iterator[Try[(DrmTaskId, DrmStatus)]] = {
      lines.iterator.map(_.trim).collect {
        case QstatRegexes.jobIdStatusAndTaskIndex(jobId, ugerStatusCode, taskIndexString) => {
          Try(DrmTaskId(jobId, taskIndexString.toInt)).map { drmTaskId =>
            val drmStatus = toDrmStatus(ugerStatusCode.trim)
            
            drmTaskId -> drmStatus
          }
        }
      }
    }

    /**
     * Map a Uger status code (as reported by qstat) to a DrmStatus
     * 
     * Parse a Uger status string into a Try[DrmStatus], based on the encoding from 
     * https://gist.github.com/cmaureir/4fa2d34bc9a1bd194af1
     * 
     * (Note that the qstat man page, which this method's implementation was based on previously, 
     * is incomplete and sometimes wrong :\ )   
     */
    def toDrmStatus(ugerStatusCode: String): DrmStatus = {
      /*
       * Pending 	pending 	qw
       * Pending 	pending, user hold 	qw
       * Pending 	pending, system hold 	hqw
       * Pending 	pending, user and system hold 	hqw
       * Pending 	pending, user hold, re-queue 	hRwq
       * Pending 	pending, system hold, re-queue 	hRwq
       * Pending 	pending, user and system hold, re-queue 	hRwq
       * Pending 	pending, user hold 	qw
       * Pending 	pending, user hold 	qw
       * Running 	running 	r
       * Running 	transferring 	t
       * Running 	running, re-submit 	Rr
       * Running 	transferring, re-submit 	Rt
       * Suspended 	obsuspended 	s, ts
       * Suspended 	queue suspended 	S, tS
       * Suspended 	queue suspended by alarm 	T, tT
       * Suspended 	allsuspended withre-submit 	Rs,Rts,RS, RtS, RT, RtT
       * Error 	allpending states with error 	Eqw, Ehqw, EhRqw
       * Deleted 	all running and suspended states with deletion 	dr,dt,dRr,dRt,ds, dS, dT,dRs, dRS, dRT
       */
      ugerStatusCode match {
        case UgerStatusCodes.IsError() | UgerStatusCodes.IsDeleted() => DrmStatus.Failed
        case UgerStatusCodes.IsQueued() => DrmStatus.Queued
        case UgerStatusCodes.IsRunning() => DrmStatus.Running
        case UgerStatusCodes.IsSuspended() => DrmStatus.Suspended
        case u => {
          warn(s"Unmapped Uger status code '${u}' mapped to '${DrmStatus.Undetermined}'")
          
          DrmStatus.Undetermined
        }
      }
    }
    
    object UgerStatusCodes {
      object IsError {
        def unapply(s: String): Boolean = s.startsWith("E") 
      }
      
      object IsDeleted {
        def unapply(s: String): Boolean = s.startsWith("d")
      }
      
      object IsQueued {
        def unapply(s: String): Boolean = s match {
          case "h" | "w" | "qw" | "hRwq" | "hqw" => true
          case _ => false
        }
      }
      
      object IsRunning {
        def unapply(s: String): Boolean = s match {
          case "r" | "R" | "t" | "Rr" | "Rt" => true
          case _ => false
        }
      }
      
      object IsSuspended {
        def unapply(s: String): Boolean = s match {
          case "s" | "S" | "N" | "ts" | "tS" | "T" | "tT" | "Rs" | "Rts" | "RS" | "RtS" | "RT" | "RtT" => true
          case _ => false
        }
      }
    }
  }
  
  private[uger] object QacctSupport {
    object Regexes {
      val exitStatus = "exit_status\\s+(.+?)$".r
      val jobNumber = "jobnumber\\s+(.+?)$".r
      val taskId = "taskid\\s+(.+?)$".r
    }
    
    //
    def parseMultiTaskQacctResults(
        idsToLookFor: Set[DrmTaskId])
       (jobNumberAndQacctLines: (String, Seq[String])): Iterable[(DrmTaskId, DrmStatus)] = {
      
      import Traversables.Implicits._
      
      def isDivider(line: String): Boolean = line.startsWith("======")
      
      val (jobNumberToLookFor, qacctLines) = jobNumberAndQacctLines
      
      val tuples = qacctLines.splitOn(isDivider).map(_.iterator.map(_.trim)).flatMap { linesForOneTask =>
        def toTry(t: (Option[String], Option[Int], Option[Int])): Try[(String, Int, Int)] = t match {
          case (Some(jn), Some(ti), Some(es)) => Success((jn, ti, es))
          case (None, _, _) => Tries.failure(s"Missing jobnumber field in ${linesForOneTask}")
          case (_, None, _) => Tries.failure(s"Missing taskid field in ${linesForOneTask}")
          case (_, _, None) => Tries.failure(s"Missing exit_status field in ${linesForOneTask}")
        }
        
        toTry(Folds.fields.process(linesForOneTask)) match {
          case Success((jobNumber, taskIndex, exitStatus)) => {
            Iterator(DrmTaskId(jobNumber, taskIndex) -> DrmStatus.CommandResult(exitStatus)).filter {
              case (drmTaskId, _) => idsToLookFor.contains(drmTaskId)
            }
          }
          case Failure(e) => {
            warn(s"Couldn't parse qacct results: ", e)
            
            Iterator.empty
          }
        }
      }
      
      tuples.toIterable
    }
    
    object Folds {
      val taskIndex = Fold.matchFirst1(Regexes.taskId).map(_.flatMap(s => Try(s.toInt).toOption))
      val jobNumber = Fold.matchFirst1(Regexes.jobNumber)
      val exitStatus = Fold.matchFirst1(Regexes.exitStatus).map(_.flatMap(s => Try(s.toInt).toOption))
      
      val fields: Fold[String, _, (Option[String], Option[Int], Option[Int])] = {
        (jobNumber |+| taskIndex |+| exitStatus).map {
          case ((jobNumber, taskIndex), exitStatus) => (jobNumber, taskIndex, exitStatus)
        }
      }
    }
  }
}
