package loamstream.drm

import loamstream.util.Loggable
import loamstream.conf.ExecutionConfig
import monix.execution.Scheduler
import loamstream.util.CommandInvoker
import loamstream.util.Terminable
import loamstream.util.ExecutorServices.QueueStrategy
import loamstream.util.ExecutorServices.RejectedExecutionStrategy
import loamstream.util.ThisMachine
import loamstream.util.Throwables
import scala.util.control.NonFatal
import monix.reactive.Observable
import scala.util.Try
import loamstream.model.jobs.DrmJobOracle
import scala.collection.compat._
import loamstream.util.RunResults
import loamstream.util.Observables
import scala.util.Success
import loamstream.util.LogContext
import loamstream.util.Processes
import loamstream.util.RateLimitedCache
import loamstream.util.CanBeClosed
import loamstream.util.Iterators
import scala.io.Source
import java.nio.file.Path
import loamstream.util.LogFileNames
import loamstream.util.Tries
import loamstream.util.FileMonitor

/**
 * @author clint
 * Jun 2, 2021
 */
abstract class RateLimitedPoller[P](
    name: String,
    protected val commandInvoker: CommandInvoker.Async[P],
    fileMonitor: FileMonitor) extends Poller with Loggable {

  import RateLimitedPoller.{PollResult, PollResultsForInvocation}
  
  protected def toParams(oracle: DrmJobOracle)(drmTaskIds: Iterable[DrmTaskId]): P
  
  protected def getStatusesByTaskId(
      idsWereLookingFor: Iterable[DrmTaskId])
     (runResults: RunResults.Completed): PollResultsForInvocation
  
  protected def getExitCodes(
      oracle: DrmJobOracle)
     (runResults: RunResults.Completed, 
      idsToLookFor: Set[DrmTaskId]): Observable[PollResult] = {
    
    def toStatusOpt(exitCodes: Iterator[Int]): Option[DrmStatus] = {
      val statuses = exitCodes.map(DrmStatus.CommandResult(_))

      import Iterators.Implicits.IteratorOps
        
      val result = statuses.nextOption()

      trace(s"Got exit status $result")

      result
    }

    def readExitCodeFromStatsFile(file: Path): Option[DrmStatus] = {
      trace(s"Reading from $file")
      
      CanBeClosed.using(Source.fromFile(file.toFile)) { source =>
        val lines: Iterator[String] = source.getLines.map(_.trim).filter(_.nonEmpty)
        
        val exitCodes: Iterator[Int] = lines.collectFirst {
          case RateLimitedPoller.Regexes.exitCodeInStatsFile(ec) => ec.toInt
        }.iterator

        toStatusOpt(exitCodes)
      }
    }

    def readExitCodeFromExitCodeFile(file: Path): Option[DrmStatus] = {
      trace(s"Reading from $file")

      CanBeClosed.using(Source.fromFile(file.toFile)) { source =>
        val lines: Iterator[String] = source.getLines.map(_.trim).filter(_.nonEmpty)
        
        val exitCodes: Iterator[Int] = {
          lines.map(line => Try(line.toInt).get)
        }

        toStatusOpt(exitCodes)
      }
    }
    
    def exitCodeFor(taskId: DrmTaskId): Observable[PollResult] = {
      def toPollResult(status: DrmStatus): PollResult = taskId -> status
      
      import java.nio.file.Files.exists

      val exitCodeFileObs = Observable.eval(oracle.dirOptFor(taskId).map(LogFileNames.exitCode))
      val statsFileObs = Observable.eval(oracle.dirOptFor(taskId).map(LogFileNames.stats))

      def waitFor(fileObs: Observable[Option[Path]]): Observable[Path] = fileObs.flatMap {
        case Some(p) => {
          trace(s"Waiting for $p") 
          Observable.from(fileMonitor.waitForCreationOf(p)).map(_ => p)
        }
        case None =>  Observable.fromTry(Tries.failure(s"Couldn't find job dir for DRM job with id: $taskId"))
      }

      val existingExitCodeFileObs = waitFor(exitCodeFileObs)
      val existingStatsFileObs = waitFor(statsFileObs)
      
      def readFromStatsFile(file: Path): Observable[DrmStatus] = Observable.fromIterable(readExitCodeFromStatsFile(file))
      def readFromExitCodeFile(file: Path): Observable[DrmStatus] = Observable.fromIterable(readExitCodeFromExitCodeFile(file))

      val statusObs = {
        for {
          ec0 <- existingStatsFileObs.flatMap(readFromStatsFile)
          ec1 <- existingExitCodeFileObs.flatMap(readFromExitCodeFile)
          ec <- Observable(ec0, ec1)
        } yield ec
      }

      statusObs.map(toPollResult)
    }

    Observable.from(idsToLookFor)
      .subscribeOn(Schedulers.oneThreadPerCpu)
      .executeOn(Schedulers.oneThreadPerCpu)
      .flatMap(exitCodeFor)
  }
  
  final override def poll(oracle: DrmJobOracle)(drmTaskIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, Try[DrmStatus])] = {
    //Invoke qstat, to get the status of all submitted-but-not-finished jobs in this session
    val commandResultObs: Observable[RunResults.Completed] = {
      import Schedulers.singleThreaded
      
      val params = toParams(oracle)(drmTaskIds)
      
      def tryAsSuccess(results: RunResults): Observable[RunResults.Completed] = {
        //TODO: Parameterize on success predicate?
        Observable.fromTry(results.tryAsSuccess("", RunResults.SuccessPredicate.zeroIsSuccess))
      }

      Observable
        .from(commandInvoker(params))
        .flatMap(tryAsSuccess)
        .observeOn(singleThreaded)
        .executeOn(singleThreaded)
        .onErrorHandleWith {
          warnThenComplete(s"Error invoking ${name}, will try again during at next polling time.")
        }
    }

    //The Set of distinct DrmTaskIds (jobId/task index coords) that we're polling for
    val drmTaskIdSet = drmTaskIds.to(Set)

    //Parse out DrmTaskIds and DrmStatuses from raw command output (one line per task)
    val pollingResultsFromCommandObs = commandResultObs
                                        .map(getStatusesByTaskId(drmTaskIdSet))
                                        .asyncBoundary(Observables.defaultOverflowStrategy)

    //For all the jobs that we're polling for that were not mentioned the underlying command, assume they've finished
    //(For example, qstat only returns info about running jobs) and look up their exit codes to determine their final 
    //statuses.
    pollingResultsFromCommandObs.flatMap { case PollResultsForInvocation(runResults, byTaskId) =>
      val notFoundDrmTaskIdSet = drmTaskIdSet -- byTaskId.keys

      if (notFoundDrmTaskIdSet.nonEmpty) {
        //For all the DrmTaskIds we're looking for but that weren't mentioned by the underlying command,
        //determine the set of distinct job ids. Or, the ids of task arrays with finished jobs
        //from the DrmTaskIds that we're polling for.
        val notFoundTaskArrayIdSet = notFoundDrmTaskIdSet.map(_.jobId)
        
        val numJobIds = notFoundTaskArrayIdSet.size

        debug(s"${notFoundDrmTaskIdSet.size} finished jobs not found by ${name}, ${numJobIds} job IDs")
      }
      
      val notFoundByTaskArrayId: Map[String, Set[DrmTaskId]] = notFoundDrmTaskIdSet.groupBy(_.jobId)
      
      val exitCodeStatusObses = {
        notFoundByTaskArrayId.values.iterator.take(ThisMachine.numCpus).map { drmTaskIdsInTaskArray =>  
          debug(s"Getting exit codes for ${drmTaskIdsInTaskArray}")

          getExitCodes(oracle)(runResults, drmTaskIdsInTaskArray)
        }.to(Seq)
      }

      val exitCodeStatuesObs = Observables.merge(exitCodeStatusObses)

      //Concatentate results from qstat with those from looking up exit codes, wrapping in Trys as needed.
      Observable.fromIterable(byTaskId) ++ {
        exitCodeStatuesObs.map { case (tid, status) => (tid, Success(status)) }
      }.asyncBoundary(Observables.defaultOverflowStrategy)
    }
  }
  
  override def stop(): Unit = {
    Throwables.quietly("Shutting down single-threaded Scheduler") {
      Schedulers.singleThreadedHandle.stop()
    }

    Throwables.quietly("Shutting down one-thread-per-CPU Scheduler") {
      Schedulers.oneThreadPerCpuHandle.stop()
    }
  }
  
  protected[drm] object Schedulers {
    lazy val (singleThreaded: Scheduler, singleThreadedHandle: Terminable) = {
      val queueSize = 5 //TODO: ???

      val (ec, handle) = loamstream.util.ExecutionContexts.singleThread(
        baseName = s"LS-${getClass.getSimpleName}-for${name}SinglePool",
        queueStrategy = QueueStrategy.Bounded(queueSize), //TODO: ???
        rejectedStrategy = RejectedExecutionStrategy.Drop) //TODO: ???
        
      (Scheduler(ec), handle)
    }

    lazy val (oneThreadPerCpu: Scheduler, oneThreadPerCpuHandle: Terminable) = {
      val queueSize = ThisMachine.numCpus //TODO: ???

      val (ec, handle) = loamstream.util.ExecutionContexts.oneThreadPerCpu(
        baseName = s"LS-${getClass.getSimpleName}-for${name}MultiPool",
        queueStrategy = QueueStrategy.Bounded(queueSize), //TODO: ???
        rejectedStrategy = RejectedExecutionStrategy.Drop) //TODO: ???
        
      (Scheduler(ec), handle)
    }
  }
  
  protected def warnThenComplete[A](msg: => String): Throwable => Observable[A] = {
    case NonFatal(e) => {
      warn(msg, e)
  
      Observable.empty
    }
  }
}

object RateLimitedPoller {
  final case class PollResultsForInvocation(runResults: RunResults.Completed, pollingResults: Map[DrmTaskId, Try[DrmStatus]])
  
  object Regexes {
    val exitCodeInStatsFile = """^ExitCode\:\s*(\d+)$""".r
  }

  type PollResult = (DrmTaskId, DrmStatus)
  
  abstract class Companion[P, A <: RateLimitedPoller[P]] {
    type Params = P
    type PollResult = RateLimitedPoller.PollResult
    type PollResultsAttempt = Try[Iterator[PollResult]]
    
    protected def commandInvoker(
      pollingFrequencyInHz: Double,
      commandName: String,
      makeTokens: P => Seq[String])
     (implicit scheduler: Scheduler, logCtx: LogContext): CommandInvoker.Async[P] = {
    
      def invocationFn(params: P): Try[RunResults] = Try {
        val tokens = makeTokens(params)
        
        logCtx.debug(s"Invoking '${tokens.mkString(" ")}'")
        
        Processes.runSync(tokens)()
      }
      
      import scala.concurrent.duration._
      
      val waitTime = (1.0 / pollingFrequencyInHz).seconds
      
      val cache = new RateLimitedCache(invocationFn, waitTime) 
      
      def cachedInvocationFn(params: Params): Try[RunResults] = cache(params)
      
      new CommandInvoker.Async.JustOnce[P](
        commandName, 
        cachedInvocationFn, isSuccess = RunResults.SuccessPredicate.zeroIsSuccess)
    }
  }
}