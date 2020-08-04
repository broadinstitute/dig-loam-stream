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

/**
 * @author clint
 * Jul 15, 2020
 */
final class QstatQacctPoller private[uger] (
    qstatInvoker: CommandInvoker.Async[Unit],
    qacctInvoker: CommandInvoker.Async[String])(implicit ec: ExecutionContext) extends Poller with Loggable {
  
  import QstatQacctPoller._
  
  override def poll(jobIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, Try[DrmStatus])] = {
    //Invoke qstat, to get the status of all submitted-but-not-finished jobs in this session
    val qstatResultObs = Observable.from(qstatInvoker.apply(()))

    //Parse out DrmTaskIds and DrmStatuses from raw qstat output
    val pollingResultsFromQstatObs = qstatResultObs.map { qstatResults => 
      QstatSupport.getByTaskId(qstatResults.stdout)
    }
    
    val jobIdSet = jobIds.toSet
    
    //For all the jobs that we're polling for but were not mentioned by qstat, assume they've finished 
    //(qstat only returns info about running jobs) and invoke qacct to determine their final status.
    pollingResultsFromQstatObs.flatMap { byTaskId =>
      val notFoundByQstat = jobIdSet -- byTaskId.keys
      
      debug(s"${notFoundByQstat.size} finished jobs not found by qstat, ${notFoundByQstat.groupBy(_.jobId).size} job IDs")
      
      val jobIdsNotFoundByQstat = notFoundByQstat.iterator.map(_.jobId).toSet
      
      //Run qstat once for each job, producing a merged stream of id -> status tuples
      //TODO: chunk up not-found task ids, to avoid running qacct too many times?
      //TODO: Run qacct in bulk?  Use -e to look up all recently-finished jobs?
      //val qacctResultsObs = Observables.merge(notFoundByQstat.iterator.map(invokeQacctFor).toIterable)
      
      
      val qacctResultsObs = Observables.merge(jobIdsNotFoundByQstat.iterator.map(invokeQacctFor(jobIdSet)).toIterable)
      
      Observable.from(byTaskId) ++ {
        qacctResultsObs.map { case (tid, status) => (tid, Success(status)) }.onBackpressureDrop
      }
    }
  }
  
  /*private def invokeQacctFor(
      drmTaskId: DrmTaskId)(implicit ec: ExecutionContext): Observable[(DrmTaskId, DrmStatus)] = {
    
    val taskIdToStatusFuture = {
      qacctInvoker(drmTaskId).map(drmTaskId -> _.stdout).map(QacctSupport.parseQacctResults).recover {
        case NonFatal(e) => drmTaskId -> DrmStatus.Undetermined 
      }
    }
    
    Observable.from(taskIdToStatusFuture)
  }*/
  
  private def invokeQacctFor(
      drmTaskIds: Set[DrmTaskId])
     (jobNumber: String)
     (implicit ec: ExecutionContext): Observable[(DrmTaskId, DrmStatus)] = {
    
    val tuplesObs = Observable.from(qacctInvoker(jobNumber).map(jobNumber -> _.stdout).map {
      QacctSupport.parseMultiTaskQacctResults(drmTaskIds)
    }).flatMap(Observable.from(_))
    
    tuplesObs.onErrorResumeNext {
      case NonFatal(e) => {
        warn(s"Error invoking qacct for task array with job id '${jobNumber}'", e)
        
        Observable.empty
      }
    }
  }
  
  override def stop(): Unit = ()
}

object QstatQacctPoller extends Loggable {
    
  def fromExecutables(
      sessionSource: SessionSource,
      actualQstatExecutable: String = "qstat",
      actualQacctExecutable: String = "qacct")(implicit ec: ExecutionContext): QstatQacctPoller = {
    
    import QacctInvoker.ByTaskArray.{ useActualBinary => qacctCommandInvoker }
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
          for {
            drmTaskId <- Try(DrmTaskId(jobId, taskIndexString.toInt))
            drmStatus <- toDrmStatus(ugerStatusCode.trim)
          } yield drmTaskId -> drmStatus
        }
      }
    }

    /**
     * Map a Uger status code (as reported by qstat) to a DrmStatus
     */
    def toDrmStatus(ugerStatusCode: String): Try[DrmStatus] = {
      /*
       * From the qstat man page:
       * 
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
    }
  }
  
  private[uger] object QacctSupport {
    object Regexes {
      val exitStatus = "exit_status\\s+(.+?)$".r
      val jobNumber = "jobnumber\\s+(.+?)$".r
      val taskId = "taskid\\s+(.+?)$".r
    }
    
    def parseMultiTaskQacctResults(
        idsToLookFor: Set[DrmTaskId])
       (t: (String, Seq[String])): Map[DrmTaskId, DrmStatus] = {
      
      import Traversables.Implicits._
      
      def isDivider(line: String): Boolean = line.startsWith("======")
      
      val (jobNumberToLookFor, lines) = t
      
      def toTry(t: (Option[String], Option[Int], Option[Int])): Try[(String, Int, Int)] = t match {
        case (Some(jn), Some(ti), Some(es)) => Success((jn, ti, es))
        case (None, _, _) => Tries.failure(s"Missing jobnumber field")
        case (_, None, _) => Tries.failure(s"Missing taskid field")
        case (_, _, None) => Tries.failure(s"Missing exit_status field")
      }
      
      val tupleAttempts = lines.splitOn(isDivider).map { linesForOneTask =>
        for {
          fieldsTuple <- getFields(linesForOneTask)
          //fieldsTuple <- toTry(Folds.fields.process(linesForOneTask))
          (jobNumber, taskIndex, exitStatus) = fieldsTuple
          drmTaskId = DrmTaskId(jobNumber, taskIndex)
          if idsToLookFor.contains(drmTaskId)
        } yield {
          drmTaskId -> DrmStatus.CommandResult(exitStatus)
        }
      }
      
      val tuples = tupleAttempts.flatMap { 
        case Success(t) => Iterator(t)
        case Failure(e) => {
          warn(s"Couldn't parse qacct results: ", e)
          
          Iterator.empty
        }
      }
      
      tuples.toMap
    }
    
    /**
     * Determine a job/task's DrmStatus from qacct output.
     * qacct doesn't return a status code, but it returns an exit code: 0 for success, anything else is a failure.
     */
    def parseQacctResults(t: (DrmTaskId, Seq[String])): (DrmTaskId, DrmStatus) = {
      import loamstream.util.Tuples.Implicits.Tuple2Ops
        
      val (tid, _) = t
      
      def parseOutputLines(lines: Seq[String]): DrmStatus = {
        getExitStatus(lines).map(DrmStatus.CommandResult).getOrElse(DrmStatus.Undetermined)
      }
      
      t.mapSecond(parseOutputLines)
    }
    
    private def findField(qacctOutput: Seq[String])(fieldName: String, regex: Regex): Try[String] = {
      val opt = qacctOutput.iterator.map(_.trim).collectFirst { 
        case regex(value) => value.trim 
      }.filter(_.nonEmpty)
      
      Options.toTry(opt)(s"Couldn't find '$fieldName' field")
    }
    
    def getExitStatus(qacctOutput: Seq[String]): Try[Int] = {
      findField(qacctOutput)("exit_status", Regexes.exitStatus).map(_.toInt)
    }
    
    private def getJobNumber(qacctOutput: Seq[String]): Try[String] = {
      findField(qacctOutput)("jobnumber", Regexes.jobNumber)
    }
    
    private def getTaskIndex(qacctOutput: Seq[String]): Try[Int] = {
      findField(qacctOutput)("taskid", Regexes.taskId).map(_.toInt)
    }
    
    private def getFields(qacctOutput: Seq[String]): Try[(String, Int, Int)] = {
      final case class State(jobNumber: Option[String], taskIndex: Option[Int], exitStatus: Option[Int]) {
        def isDone: Boolean = jobNumber.isDefined && taskIndex.isDefined && exitStatus.isDefined
        
        def toTry: Try[(String, Int, Int)] = this match {
          case State(Some(jn), Some(ti), Some(es)) => Success((jn, ti, es))
          case State(None, _, _) => Tries.failure(s"Missing jobnumber field")
          case State(_, None, _) => Tries.failure(s"Missing taskid field")
          case State(_, _, None) => Tries.failure(s"Missing exit_status field")
        }
        
        def withJobNumber(jn: String): State = copy(jobNumber = Option(jn))
        def withTaskIndex(ti: => Int): State = copy(taskIndex = Try(ti).toOption)
        def withExitStatus(es: => Int): State = copy(exitStatus = Try(es).toOption)
      }
      
      @tailrec
      def loop(acc: State, remaining: Seq[String]): State = {
        if(remaining.isEmpty) { acc }
        else {
          remaining.head match {
            case Regexes.jobNumber(jn) => {
              val newAcc = acc.withJobNumber(jn.trim)
              if(newAcc.isDone) newAcc else loop(newAcc, remaining.tail)
            }
            case Regexes.taskId(tid) =>  {
              val newAcc = acc.withTaskIndex(tid.trim.toInt)
              if(newAcc.isDone) newAcc else loop(newAcc, remaining.tail)
            }
            case Regexes.exitStatus(es) => {
              val newAcc = acc.withExitStatus(es.trim.toInt)
              if(newAcc.isDone) newAcc else loop(newAcc, remaining.tail)
            }
            case _ => loop(acc, remaining.tail)
          }
        }
      }
      
      val x: (Option[String], Option[Int], Option[Int]) = Folds.fields.process(qacctOutput)
      
      loop(State(None, None, None), qacctOutput).toTry
    }
    
    def getDrmTaskId(qacctOutput: Seq[String]): Try[DrmTaskId] = {
      for {
        jobNumber <- getJobNumber(qacctOutput)
        taskId <- getTaskIndex(qacctOutput)
      } yield DrmTaskId(jobNumber, taskId)
    }
    
    object Folds {
      val taskIndex = Fold.matchFirst1(Regexes.taskId).map(_.flatMap(s => Try(s.toInt).toOption))
      val jobNumber = Fold.matchFirst1(Regexes.jobNumber)
      val exitStatus = Fold.matchFirst1(Regexes.exitStatus).map(_.flatMap(s => Try(s.toInt).toOption))
      
      val drmTaskId = (jobNumber |+| taskIndex).map { 
        case (Some(jn), Some(ti)) => Some(DrmTaskId(jn, ti))
        case _ => None
      }
      
      val fields: Fold[String, _, (Option[String], Option[Int], Option[Int])] = (jobNumber |+| taskIndex |+| exitStatus).map {
        case ((jn, ti), es) => (jn, ti, es)
        case _ => (None, None, None)
      }
    }
  }
}
