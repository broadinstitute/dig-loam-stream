package loamstream.model.execute

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationDouble

import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobRun
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import loamstream.util.Maps
import loamstream.util.Observables
import rx.lang.scala.Observable
import rx.lang.scala.Scheduler
import rx.lang.scala.schedulers.IOScheduler
import loamstream.model.jobs.JobNode
import loamstream.conf.ExecutionConfig
import loamstream.util.ValueBox
import java.io.FileWriter
import loamstream.util.ExecutionContexts
import loamstream.util.Terminable
import java.io.PrintWriter
import java.time.Instant
import loamstream.model.jobs.log.JobLog
import loamstream.model.jobs.RunData
import scala.concurrent.Future
import loamstream.conf.LoamConfig
import loamstream.util.FileMonitor
import loamstream.util.Futures
import jdk.nashorn.internal.runtime.FinalScriptFunctionData
import loamstream.model.jobs.JobOracle
import loamstream.util.TimeUtils

/**
 * @author kaan
 * @author clint
 *         date: Aug 17, 2016
 */
final case class RxExecuter(
    executionConfig: ExecutionConfig,
    runner: ChunkRunner,
    fileMonitor: FileMonitor,
    windowLength: Duration,
    jobCanceler: JobCanceler,
    jobFilter: JobFilter,
    executionRecorder: ExecutionRecorder,
    maxRunsPerJob: Int,
    override protected val terminableComponents: Iterable[Terminable] = Nil)
    (implicit val executionContext: ExecutionContext) extends Executer with Terminable.StopsComponents with Loggable {
  
  require(maxRunsPerJob >= 1, s"The maximum number of times to run each job must not be negative; got $maxRunsPerJob")
  
  import executionRecorder.record
  import loamstream.util.Observables.Implicits._
  
  override def execute(
      executable: Executable, 
      makeJobOracle: Executable => JobOracle = JobOracle.fromExecutable(executionConfig, _))
     (implicit timeout: Duration = Duration.Inf): Map[LJob, Execution] = {
    
    val jobOracle = makeJobOracle(executable)
    
    val numJobs = executable.allJobs.size
    
    val executionState: ExecutionState = TimeUtils.time(s"Initializing execution state with ${numJobs} jobs", debug(_)) {
      ExecutionState.initialFor(executable, maxRunsPerJob)
    }
    
    def finish(results: Map[LJob, Execution]): Unit = {
      TimeUtils.time(s"Finishing ${results.size} jobs", debug(_)) {
        results.foreach {case (job, execution) => executionState.finish(job, execution) }
      }
    }
    
    def runEligibleJobs(): Observable[Map[LJob, Execution]] = {
      
      val jobsAndCells = executionState.updateJobs()
      
      val numReadyToRun = jobsAndCells.readyToRun.size
      val numCannotRun = jobsAndCells.cannotRun.size
      import jobsAndCells.{ numRunning, numFinished }
      
      val numRemaining = numJobs - numReadyToRun - numCannotRun - numRunning - numFinished
      
      debug(s"RxExecuter.runEligibleJobs(): $numReadyToRun jobs ready to run; $numCannotRun jobs cannot run; $numRunning running; $numFinished finishedl; $numRemaining other.")
      
      val (finishedJobAndCells, notFinishedJobsAndCells) = {
        jobsAndCells.readyToRun.partition { case (_, cell) => cell.isTerminal }
      }
      
      val finishedJobs = finishedJobAndCells.keys
      val notFinishedJobs = notFinishedJobsAndCells.keys
      
      if(notFinishedJobs.nonEmpty) {
        val (jobsToMaybeRun, skippedJobs) = notFinishedJobs.map(_.job).partition(jobFilter.shouldRun)
        
        val (jobsToCancel, jobsToRun) = jobsToMaybeRun.partition(jobCanceler.shouldCancel)
        
        handleSkippedJobs(skippedJobs)
        
        val cancelledJobsMap = cancelJobs(jobsToCancel)
        
        record(jobOracle, cancelledJobsMap)
        
        val skippedResultMap = toSkippedResultMap(skippedJobs)
        
        for {
          executionTupleOpt <- runJobs(jobsToRun, jobOracle)
        } yield {
          record(jobOracle, executionTupleOpt)
          
          val executionMap = cancelledJobsMap ++ executionTupleOpt
          
          logFinishedJobs(executionMap)
          
          val results = executionMap ++ skippedResultMap
          
          finish(results)
          
          results
        }
      } else {
        Observable.just(Map.empty)
      }
    }
    
    val ioScheduler: Scheduler = IOScheduler()
    
    val chunkResults = Observable.interval(windowLength, ioScheduler).flatMap(_ => runEligibleJobs()).takeUntil(_ => executionState.isFinished)
    
    val futureMergedResults = chunkResults.foldLeft(emptyExecutionMap)(_ ++ _).firstAsFuture

    Await.result(futureMergedResults, timeout)
  }

  private val emptyExecutionMap: Map[LJob, Execution] = Map.empty
  
  private def logFinishedJobs(jobs: Map[LJob, Execution]): Unit = {
    for {
      (job, execution) <- jobs
    } {
      info(s"Finished with ${execution.status} (${execution.result}) when running $job")
    }
  }
  
  //Produce Optional LJob -> Execution tuples.  We need to be able to produce just one (empty) item,
  //instead of just returning Observable.empty, so that code chained onto this method's result with
  //flatMap will run.
  private def runJobs(jobsToRun: Iterable[LJob], jobOracle: JobOracle): Observable[Option[(LJob, Execution)]] = {
    logJobsToBeRun(jobsToRun)
    
    import RxExecuter.toExecutionMap
    
    val emptyMap = Map.empty[LJob, Execution]
    
    if(jobsToRun.isEmpty) { Observable.just(None) }
    else {
      val jobRunObs = runner.run(jobsToRun.toSet, jobOracle)
      
      jobRunObs.flatMap(toExecutionMap(fileMonitor)).map(Option(_))
    }
  }
  
  private def cancelJobs(jobsToCancel: Iterable[LJob]): Map[LJob, Execution] = {
    import JobStatus.Canceled
    
    import loamstream.util.Traversables.Implicits._
    
    jobsToCancel.mapTo(job => Execution.from(job, Canceled, terminationReason = None))
  }
  
  private def handleSkippedJobs(skippedJobs: Iterable[LJob]): Unit = {
    logSkippedJobs(skippedJobs)
  }
  
  private def logJobsToBeRun(jobsToRun: Iterable[LJob]): Unit = {
    debug(s"Dispatching (${jobsToRun.size}) jobs to ChunkRunner:")
    
    jobsToRun.foreach(job => debug(s"Dispatching job to ChunkRunner: $job"))
  }
  
  private def logSkippedJobs(skippedJobs: Iterable[LJob]): Unit = skippedJobs.size match {
    case 0 => debug("Skipped 0 jobs")
    case numSkipped => {
      info(s"Skipped ($numSkipped) jobs:")
    
      skippedJobs.foreach(job => info(s"Skipped: $job"))
    }
  }
  
  private def toSkippedResultMap(skippedJobs: Iterable[LJob]): Map[LJob, Execution] = {
    import loamstream.util.Traversables.Implicits._
      
    skippedJobs.mapTo(job => Execution.from(job, JobStatus.Skipped, terminationReason = None))
  }
}

object RxExecuter extends Loggable {
  object Defaults {
    //NB: Use a short windowLength to speed up tests
    val windowLength: Double = 0.05
    val windowLengthInSec: Duration = windowLength.seconds
  
    val jobCanceler: JobCanceler = JobCanceler.NeverCancel
    
    val jobFilter: JobFilter = JobFilter.RunEverything
  
    val executionRecorder: ExecutionRecorder = ExecutionRecorder.DontRecord
    
    val maxRunsPerJob: Int = 4
    
    val executionConfig: ExecutionConfig = ExecutionConfig.default
    
    lazy val maxNumConcurrentJobs: Int = AsyncLocalChunkRunner.defaultMaxNumJobs
    
    lazy val maxWaitTimeForOutputs: Duration = executionConfig.maxWaitTimeForOutputs
    
    lazy val outputExistencePollingFrequencyInHz: Double = executionConfig.outputPollingFrequencyInHz
    
    lazy val fileMonitor: FileMonitor = new FileMonitor(outputExistencePollingFrequencyInHz, maxWaitTimeForOutputs)
  }
  
  def apply(runner: ChunkRunner)(implicit executionContext: ExecutionContext): RxExecuter = {
    new RxExecuter(
        Defaults.executionConfig,
        runner, 
        Defaults.fileMonitor, 
        Defaults.windowLengthInSec,
        Defaults.jobCanceler,
        Defaults.jobFilter, 
        Defaults.executionRecorder,
        Defaults.maxRunsPerJob)
  }

  def default: RxExecuter = {
    val (executionContext, ecHandle) = ExecutionContexts.threadPool(Defaults.maxNumConcurrentJobs)

    val chunkRunner = AsyncLocalChunkRunner(Defaults.executionConfig, Defaults.maxNumConcurrentJobs)(executionContext)

    new RxExecuter(
        Defaults.executionConfig,
        chunkRunner, 
        Defaults.fileMonitor,
        Defaults.windowLengthInSec, 
        Defaults.jobCanceler,
        Defaults.jobFilter, 
        Defaults.executionRecorder,
        Defaults.maxRunsPerJob, 
        Option(ecHandle))(executionContext)
  }
  
  def defaultWith(
      newJobFilter: JobFilter = Defaults.jobFilter, 
      newExecutionRecorder: ExecutionRecorder = Defaults.executionRecorder): RxExecuter = {
    
    def addJobFilter(rxe: RxExecuter): RxExecuter = {
      implicit val context = rxe.executionContext 
      
      if(newJobFilter eq rxe.jobFilter) rxe else rxe.copy(jobFilter = newJobFilter)
    }
    
    def addExecutionRecorder(rxe: RxExecuter): RxExecuter = {
      implicit val context = rxe.executionContext
      
      if(newExecutionRecorder eq rxe.executionRecorder) rxe else rxe.copy(executionRecorder = newExecutionRecorder)
    }
    
    addExecutionRecorder(addJobFilter(default))
  }

  /**
   * Turns the passed `runDataMap` into an observable that will fire once, producing a Map derived from `runDataMap`
   * by turning `runDataMap`'s values into Executions after waiting for any missing outputs. 
   */
  private[execute] def toExecutionMap(
      fileMonitor: FileMonitor)
      (runDataMap: Map[LJob, RunData])
      (implicit context: ExecutionContext): Observable[(LJob, Execution)] = {
  
    def waitForOutputs(runData: RunData): Future[Execution] = {
      ExecuterHelpers.waitForOutputsAndMakeExecution(runData, fileMonitor)
    }
    
    val jobToExecutionFutures = for {
      (job, runData) <- runDataMap.toSeq
    } yield {
      import Futures.Implicits._
      
      waitForOutputs(runData).map(execution => job -> execution)
    }
    
    Observable.from(jobToExecutionFutures).flatMap(Observable.from(_))
  }
}
