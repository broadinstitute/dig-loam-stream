package loamstream.drm.uger

import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import loamstream.conf.UgerConfig
import loamstream.drm.DrmStatus
import loamstream.drm.DrmTaskId
import loamstream.drm.Poller
import loamstream.util.CommandInvoker
import loamstream.util.ExecutorServices.QueueStrategy
import loamstream.util.ExecutorServices.RejectedExecutionStrategy
import loamstream.util.Fold
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.Terminable
import loamstream.util.ThisMachine
import loamstream.util.Throwables
import loamstream.util.Tries
import rx.lang.scala.Observable
import rx.lang.scala.Scheduler
import rx.lang.scala.schedulers.ExecutionContextScheduler
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.DrmJobOracle
import java.nio.file.Path
import loamstream.util.LogFileNames
import loamstream.util.CanBeClosed
import scala.io.Source
import loamstream.util.Iterators
import loamstream.util.ValueBox

/**
 * @author clint
 * Jul 15, 2020
 */
final class QstatPoller private[uger] (qstatInvoker: CommandInvoker.Async[Unit]) extends Poller with Loggable {

  import QstatPoller._

  override def poll(oracle: DrmJobOracle)(drmTaskIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, Try[DrmStatus])] = {
    //Invoke qstat, to get the status of all submitted-but-not-finished jobs in this session
    val qstatResultObs = {
      implicit val ec = ExecutionContexts.forQstat

      Observable.from(qstatInvoker.apply(())).observeOn(Schedulers.forQstat).onErrorResumeNext {
        warnThenComplete(s"Error invoking qstat, will try again at next polling time.")
      }
    }

    //The Set of distinct DrmTaskIds (jobId/task index coords) that we're polling for
    val drmTaskIdSet = drmTaskIds.toSet

    //Parse out DrmTaskIds and DrmStatuses from raw qstat output (one line per task)
    val pollingResultsFromQstatObs = qstatResultObs.map { qstatResults =>
      QstatSupport.getByTaskId(drmTaskIdSet, qstatResults.stdout)
    }.onBackpressureDrop

    //For all the jobs that we're polling for that were not mentioned by qstat, assume they've finished
    //(qstat only returns info about running jobs) and look up their exit codes to determine their final statuses.
    pollingResultsFromQstatObs.flatMap { byTaskId =>
      val notFoundByQstat = drmTaskIdSet -- byTaskId.keys

      if (notFoundByQstat.nonEmpty) {
        //For all the DrmTaskIds we're looking for but that weren't mentioned by qstat,
        //determine the set of distinct job ids. Or, the ids of task arrays with finished jobs
        //from the DrmTaskIds that we're polling for.
        val taskArrayIdsNotFoundByQstat = notFoundByQstat.map(_.jobId)
        
        val numJobIds = taskArrayIdsNotFoundByQstat.size

        debug(s"${notFoundByQstat.size} finished jobs not found by qstat, ${numJobIds} job IDs")
      }
      
      val notFoundByQstatByTaskArrayId: Map[String, Set[DrmTaskId]] = notFoundByQstat.groupBy(_.jobId)
      
      val exitCodeStatusObses = {
        notFoundByQstatByTaskArrayId.values.iterator.take(ThisMachine.numCpus).map { drmTaskIdsInTaskArray =>  
          getExitCodes(oracle)(drmTaskIdsInTaskArray)
        }.toSeq
      }

      val exitCodeStatuesObs = Observables.merge(exitCodeStatusObses)

      //Concatentate results from qstat with those from looking up exit codes, wrapping in Trys as needed.
      Observable.from(byTaskId) ++ {
        exitCodeStatuesObs.map { case (tid, status) => (tid, Success(status)) }
      }.onBackpressureDrop
    }
  }

  private def getExitCodes(oracle: DrmJobOracle)(idsToLookFor: Set[DrmTaskId]): Observable[PollResult] = {
    def readExitCodeFrom(file: Path): Option[DrmStatus] = {
      CanBeClosed.using(Source.fromFile(file.toFile)) { source =>
        import Iterators.Implicits.IteratorOps
        
        val lines: Iterator[String] = source.getLines.map(_.trim).filter(_.nonEmpty)
        
        val statuses: Iterator[DrmStatus] = {
          lines.flatMap(line => Try(line.toInt).toOption).map(DrmStatus.CommandResult(_))
        }
        
        statuses.nextOption()
      }
    }
    
    def exitCodeFor(taskId: DrmTaskId): Observable[PollResult] = {
      def toPollResult(status: DrmStatus): PollResult = taskId -> status
      
      import java.nio.file.Files.exists

      val exitCodeFile: Option[Path] = oracle.dirOptFor(taskId).map(LogFileNames.exitCode)
      
      Observable.from {
        exitCodeFile.flatMap { file =>
          if(exists(file)) { readExitCodeFrom(file).map(toPollResult) } 
          else { None }
        }
      }
    }
    
    Observable.from(idsToLookFor).subscribeOn(Schedulers.forExitStatuses).flatMap(exitCodeFor)
  }
  
  private def warnThenComplete[A](msg: => String): Throwable => Observable[A] = {
    case NonFatal(e) => {
      warn(msg, e)
  
      Observable.empty
    }
  }
  
  override def stop(): Unit = {
    Throwables.quietly("Shutting down Qstat ExecutionContext") {
      ExecutionContexts.forQstatHandle.stop()
    }

    Throwables.quietly("Shutting down exit-status-lookup ExecutionContext") {
      ExecutionContexts.forExitStatusesHandle.stop()
    }
  }

  private[uger] object ExecutionContexts {
    lazy val (forQstat: ExecutionContext, forQstatHandle: Terminable) = {
      val queueSize = 5 //TODO: ???

      loamstream.util.ExecutionContexts.singleThread(
        baseName = "LS-QstatQAcctPoller-forQstatPool",
        queueStrategy = QueueStrategy.Bounded(queueSize), //TODO: ???
        rejectedStrategy = RejectedExecutionStrategy.Drop) //TODO: ???
    }

    lazy val (forExitStatuses: ExecutionContext, forExitStatusesHandle: Terminable) = {
      val queueSize = ThisMachine.numCpus //TODO: ???

      loamstream.util.ExecutionContexts.oneThreadPerCpu(
        baseName = "LS-QstatQAcctPoller-forQacctPool",
        queueStrategy = QueueStrategy.Bounded(queueSize), //TODO: ???
        rejectedStrategy = RejectedExecutionStrategy.Drop) //TODO: ???
    }
  }

  private[uger] object Schedulers {
    val forQstat: Scheduler = ExecutionContextScheduler(ExecutionContexts.forQstat)

    val forExitStatuses: Scheduler = ExecutionContextScheduler(ExecutionContexts.forExitStatuses)
  }
}

object QstatPoller extends Loggable {

  def fromExecutables(
    qstatPollingFrequencyInHz: Double,
    ugerConfig: UgerConfig,
    actualQstatExecutable: String = "qstat",
    scheduler: Scheduler)(implicit ec: ExecutionContext): QstatPoller = {

    import QacctInvoker.ByTaskArray.{ useActualBinary => qacctCommandInvoker }
    import Qstat.{ commandInvoker => qstatCommandInvoker }
    import scala.concurrent.duration._

    val qstat = qstatCommandInvoker(qstatPollingFrequencyInHz, actualQstatExecutable)

    new QstatPoller(qstat)
  }

  type PollResult = (DrmTaskId, DrmStatus)
  type PollResultsAttempt = Try[Iterator[PollResult]]
  
  private[uger] object QstatSupport {
    object QstatRegexes {
      val jobIdStatusAndTaskIndex = """^(\w+)\s+\S+\s+\S+\s+\S+\s+(\w+)\s+.+\d+\s+(\d+)$""".r
      val jobIdStatusForWholeTaskArray = """^(\w+)\s+\S+\s+\S+\s+\S+\s+(\w+)\s+.+(\d+)\-(\d+)\:(\d+)$""".r
    }

    def getByTaskId(
      idsWereLookingFor: Iterable[DrmTaskId],
      qstatOutput: Seq[String]): Map[DrmTaskId, Try[DrmStatus]] = {

      parseQstatOutput(idsWereLookingFor, qstatOutput).collect {
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
    def parseQstatOutput(
      idsWereLookingFor: Iterable[DrmTaskId],
      lines: Seq[String]): Iterator[Try[PollResult]] = {

      val idsWereLookingForSet = idsWereLookingFor.toSet

      val isIdWeCareAbout: PollResult => Boolean = {
        case (taskId, _) => idsWereLookingForSet.contains(taskId)
      }

      val lineResults: Iterator[PollResultsAttempt] = lines.iterator.map(_.trim).collect {
        case QstatRegexes.jobIdStatusAndTaskIndex(jobId, ugerStatusCode, taskIndexString) => {
          handleSingleJobLine(jobId, ugerStatusCode, taskIndexString, isIdWeCareAbout)
        }
        case QstatRegexes.jobIdStatusForWholeTaskArray(jobId, ugerStatusCode, idxStart, idxEnd, idxIncrement) => {
          handleTaskArrayLine(jobId, ugerStatusCode, idxStart, idxEnd, idxIncrement, isIdWeCareAbout)
        }
      }

      lineResults.flatMap {
        case Success(tuples) => tuples.map(Success(_))
        case Failure(e) => Iterator(Failure(e))
      }
    }
    
    private def handleSingleJobLine(
        jobId: String, 
        ugerStatusCode: String, 
        taskIndexString: String,
        isIdWeCareAbout: ((DrmTaskId, DrmStatus)) => Boolean): PollResultsAttempt = {
      
      Try(DrmTaskId(jobId, taskIndexString.toInt)).map { drmTaskId =>
        val drmStatus = toDrmStatus(ugerStatusCode.trim)

        Iterator(drmTaskId -> drmStatus).filter(isIdWeCareAbout)
      }
    }
      
    private def handleTaskArrayLine(
        jobId: String, 
        ugerStatusCode: String, 
        idxStart: String, 
        idxEnd: String, 
        idxIncrement: String,
        isIdWeCareAbout: ((DrmTaskId, DrmStatus)) => Boolean): PollResultsAttempt = {

      for {
        start <- Try(idxStart.trim.toInt)
        end <- Try(idxEnd.trim.toInt)
        increment <- Try(idxIncrement.trim.toInt)
      } yield {
        val drmStatus = toDrmStatus(ugerStatusCode.trim)

        def toPollResult(taskIndex: Int): PollResult = DrmTaskId(jobId.trim, taskIndex) -> drmStatus
        
        (start to end by increment).iterator.map(toPollResult).filter(isIdWeCareAbout)
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
       * Pending   pending qw
       * Pending   pending, user hold qw
       * Pending   pending, system hold   hqw
       * Pending   pending, user and system hold   hqw
       * Pending   pending, user hold, re-queue   hRwq
       * Pending   pending, system hold, re-queue   hRwq
       * Pending   pending, user and system hold, re-queue   hRwq
       * Pending   pending, user hold   qw
       * Pending   pending, user hold   qw
       * Running   running   r
       * Running   transferring   t
       * Running   running, re-submit   Rr
       * Running   transferring, re-submit   Rt
       * Suspended   obsuspended   s, ts
       * Suspended   queue suspended   S, tS
       * Suspended   queue suspended by alarm   T, tT
       * Suspended   allsuspended withre-submit   Rs,Rts,RS, RtS, RT, RtT
       * Error   allpending states with error   Eqw, Ehqw, EhRqw
       * Deleted   all running and suspended states with deletion   dr,dt,dRr,dRt,ds, dS, dT,dRs, dRS, dRT
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
}
