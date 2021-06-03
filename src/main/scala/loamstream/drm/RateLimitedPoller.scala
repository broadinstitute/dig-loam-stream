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

/**
 * @author clint
 * Jun 2, 2021
 */
abstract class RateLimitedPoller[P](
    name: String,
    protected val commandInvoker: CommandInvoker.Async[P]) extends Poller with Loggable {

  import RateLimitedPoller.{PollResult, PollResultsForInvocation}
  
  protected def toParams(oracle: DrmJobOracle)(drmTaskIds: Iterable[DrmTaskId]): P
  
  protected def getStatusesByTaskId(
      idsWereLookingFor: Iterable[DrmTaskId])
     (runResults: RunResults.Successful): PollResultsForInvocation
  
  protected def getExitCodes(
      oracle: DrmJobOracle)
     (runResults: RunResults.Successful, 
      idsToLookFor: Set[DrmTaskId]): Observable[PollResult] = Observable.empty
  
  final override def poll(oracle: DrmJobOracle)(drmTaskIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, Try[DrmStatus])] = {
    //Invoke qstat, to get the status of all submitted-but-not-finished jobs in this session
    val commandResultObs: Observable[RunResults.Successful] = {
      import Schedulers.singleThreaded
      
      val params = toParams(oracle)(drmTaskIds)
      
      Observable.from(commandInvoker(params)).observeOn(singleThreaded).executeOn(singleThreaded).onErrorHandleWith {
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
  final case class PollResultsForInvocation(runResults: RunResults.Successful, pollingResults: Map[DrmTaskId, Try[DrmStatus]])
  
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
    
      def invocationFn(params: P): Try[RunResults] = {
        val tokens = makeTokens(params)
        
        logCtx.trace(s"Invoking '${tokens.mkString(" ")}'")
        
        Processes.runSync(tokens)()
      }
      
      import scala.concurrent.duration._
      
      val waitTime = (1.0 / pollingFrequencyInHz).seconds
      
      val cache = new RateLimitedCache(invocationFn, waitTime) 
      
      def cachedInvocationFn(params: Params): Try[RunResults] = cache(params)
      
      new CommandInvoker.Async.JustOnce[P](commandName, cachedInvocationFn)
    }
  }
}