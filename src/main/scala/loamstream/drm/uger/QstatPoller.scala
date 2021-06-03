package loamstream.drm.uger

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import QstatPoller.Params
import loamstream.conf.ExecutionConfig
import loamstream.drm.DrmStatus
import loamstream.drm.DrmTaskId
import loamstream.drm.RateLimitedPoller
import loamstream.drm.RateLimitedPoller.PollResultsForInvocation
import loamstream.model.jobs.DrmJobOracle
import loamstream.util.CommandInvoker
import loamstream.util.FileMonitor
import loamstream.util.Loggable
import monix.execution.Scheduler

import scala.collection.compat._
import loamstream.util.RunResults
import monix.reactive.Observable
import java.nio.file.Path
import scala.io.Source
import loamstream.util.CanBeClosed
import loamstream.util.Iterators
import loamstream.util.LogFileNames
import loamstream.util.Tries


final class QstatPoller private[uger] (
    commandName: String,
    pollingFn: CommandInvoker.Async[Unit],
    fileMonitor: FileMonitor) extends RateLimitedPoller[Unit](commandName, pollingFn) {

  override protected def toParams(oracle: DrmJobOracle)(drmTaskIds: Iterable[DrmTaskId]): Params = ()
  
  import QstatPoller.PollResult
  import QstatPoller.QstatSupport
  
  override protected def getStatusesByTaskId(
      idsWereLookingFor: Iterable[DrmTaskId])
     (runResults: RunResults.Successful): PollResultsForInvocation = {
    
    val attemptsByTaskId = QstatSupport.getByTaskId(idsWereLookingFor, runResults.stdout)
    
    PollResultsForInvocation(runResults, attemptsByTaskId)
  }
  
  override protected def getExitCodes(
      oracle: DrmJobOracle)
     (runResults: RunResults.Successful, 
      idsToLookFor: Set[DrmTaskId]): Observable[PollResult] = {
    
    def readExitCodeFrom(file: Path): Option[DrmStatus] = {
      CanBeClosed.using(Source.fromFile(file.toFile)) { source =>
        val lines: Iterator[String] = source.getLines.map(_.trim).filter(_.nonEmpty)
        
        val statuses: Iterator[DrmStatus] = {
          lines.flatMap(line => Try(line.toInt).toOption).map(DrmStatus.CommandResult(_))
        }

        import Iterators.Implicits.IteratorOps
        
        statuses.nextOption()
      }
    }
    
    def exitCodeFor(taskId: DrmTaskId): Observable[PollResult] = {
      def toPollResult(status: DrmStatus): PollResult = taskId -> status
      
      import java.nio.file.Files.exists

      val exitCodeFileObs = Observable.eval(oracle.dirOptFor(taskId).map(LogFileNames.exitCode))
      
      val existingExitCodeFileObs = exitCodeFileObs.flatMap {
        case Some(p) => Observable.from(fileMonitor.waitForCreationOf(p)).map(_ => p)
        case None => Observable.fromTry(Tries.failure(s"Couldn't find job dir for DRM job with id: $taskId"))
      }
      
      existingExitCodeFileObs.flatMap { file =>
        Observable.fromIterable(readExitCodeFrom(file).map(toPollResult))
      }
    }
    
    Observable.from(idsToLookFor)
      .subscribeOn(Schedulers.oneThreadPerCpu)
      .executeOn(Schedulers.oneThreadPerCpu)
      .flatMap(exitCodeFor)
  }
}

object QstatPoller extends RateLimitedPoller.Companion[Unit, QstatPoller] with Loggable {
  def fromExecutable(
      pollingFrequencyInHz: Double,
      executionConfig: ExecutionConfig,
      actualExecutable: String = "qstat",
      scheduler: Scheduler): QstatPoller = {
    
    val invoker = commandInvoker(
        pollingFrequencyInHz, 
        actualExecutable, 
        _ => Qstat.makeTokens(actualExecutable))(scheduler, this)
    
    import executionConfig.{ executionPollingFrequencyInHz, maxWaitTimeForOutputs }
    
    val fileMonitor = new FileMonitor(executionPollingFrequencyInHz, maxWaitTimeForOutputs)
    
    new QstatPoller(actualExecutable, invoker, fileMonitor)
  }
      
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

      val idsWereLookingForSet = idsWereLookingFor.to(Set)

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