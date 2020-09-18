package loamstream.model.execute

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationDouble

import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.util.ExecutionContexts
import loamstream.util.FileMonitor
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.Terminable
import loamstream.util.TimeUtils
import rx.lang.scala.Observable
import rx.lang.scala.Scheduler
import rx.lang.scala.schedulers.IOScheduler


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
    scheduler: Scheduler,
    override protected val terminableComponents: Iterable[Terminable] = Nil)
    (implicit val executionContext: ExecutionContext) extends Executer with Terminable.StopsComponents with Loggable {
  
  require(maxRunsPerJob >= 1, s"The maximum number of times to run each job must not be negative; got $maxRunsPerJob")
  
  import executionRecorder.record
  import loamstream.util.Observables.Implicits._
  
  /**
   * Execute all the jobs in an Executable, blocking until every one of them that can be run has been run.
   * 
   * At a high level: 
   *  - Track the state of every job
   *  - Poll the status of every job
   *  - When a job becomes runnable, run it
   *  - When a job fails, mark all its successors as CouldNotStart
   *  - Repeat until all jobs are done or could not start 
   */
  override def execute(
      executable: Executable, 
      makeJobOracle: Executable => JobOracle = JobOracle.fromExecutable(executionConfig, _))
     (implicit timeout: Duration = Duration.Inf): Map[LJob, Execution] = {
    
    val jobOracle = makeJobOracle(executable)
    
    def numJobs = executable.allJobs.size
    
    val executionState = TimeUtils.time(s"Initializing execution state with ${numJobs} jobs", debug(_)) {
      ExecutionState.initialFor(executable, maxRunsPerJob)
    }
    
    val jobResultTuples: Observable[(LJob, Execution)] = {
      //Note onBackpressureDrop(), in case runEligibleJobs takes too long (or the polling window is too short)
      val ticks = Observable.interval(windowLength, scheduler).onBackpressureDrop
      
      def runJobs(jobsAndCells: ExecutionState.JobStatuses) = runEligibleJobs(executionState, jobOracle, jobsAndCells)
      
      def isFinished = executionState.isFinished
      
      ticks.map(_ => executionState.updateJobs()).distinctUntilChanged.flatMap(runJobs).takeUntil(_ => isFinished)
    }
    
    val futureMergedResults = jobResultTuples.toMap.firstAsFuture

    Await.result(futureMergedResults, timeout)
  }
  
  private def finishOneJob(
      executionState: ExecutionState)(result: (LJob, Execution)): (LJob, Execution) = {
    
    val (job, execution) = result
    
    executionState.finish(job, execution.status)
    
    result
  }
  
  private def finish(
      executionState: ExecutionState)(results: Iterable[(LJob, Execution)]): Observable[(LJob, Execution)] = {
    
    if(results.isEmpty) { Observable.empty }
    else {
      def msg = {
        val howMany: Int = 50 //scalastyle:ignore magic.number
  
        def firstIds = results.iterator.collect { case (j, _) => j.id }
        
        s"Finishing ${results.size} jobs; first $howMany ids: ${firstIds.mkString(",")}"
      }
      
      TimeUtils.time(msg, trace(_)) {
        Observable.from(results).map(finishOneJob(executionState))
      }
    }
  }
  
  private def runEligibleJobs(
      executionState: ExecutionState, 
      jobOracle: JobOracle,
      jobsAndCells: ExecutionState.JobStatuses): Observable[(LJob, Execution)] = {
    
    val (numReadyToRun, numCannotRun) = (jobsAndCells.readyToRun.size, jobsAndCells.cannotRun.size)
    import jobsAndCells.{ numFinished, numRunning }
    val numRemaining = executionState.size - numReadyToRun - numCannotRun - numRunning - numFinished
    
    info(s"RxExecuter.runEligibleJobs(): Ready to run: $numReadyToRun; Cannot run: $numCannotRun; " +
         s"Running: $numRunning; Finished: $numFinished; Other: $numRemaining.")
    
    val (finishedJobStates, notFinishedJobStates) = {
      jobsAndCells.readyToRun.partition(_.isTerminal)
    }
    
    val (finishedJobs, notFinishedJobs) = (finishedJobStates.map(_.job), notFinishedJobStates.map(_.job))
    
    if(notFinishedJobs.nonEmpty) {
      runNotFinishedJobs(notFinishedJobs, executionState, jobOracle)
    } else {
      Observable.empty
    }
  }
  
  private def runNotFinishedJobs(
      notFinishedJobs: Iterable[LJob],
      executionState: ExecutionState, 
      jobOracle: JobOracle): Observable[(LJob, Execution)] = {
    
    val (jobsToMaybeRun, skippedJobs) = notFinishedJobs.map(_.job).partition(jobFilter.shouldRun)
      
    val (jobsToCancel, jobsToRun) = jobsToMaybeRun.partition(jobCanceler.shouldCancel)
    
    handleSkipped(skippedJobs)
    
    val cancelledJobTuples = cancelJobs(jobsToCancel)
    
    record(jobOracle, cancelledJobTuples)
    
    val skippedResultTuples = toSkippedResultTuples(skippedJobs)
    
    logFinished(cancelledJobTuples)
    logFinished(skippedResultTuples)
    
    val cancelledOrSkippedJobsObs = finish(executionState)(cancelledJobTuples ++ skippedResultTuples)
    
    val actuallyRunJobsObs = runJobs(jobsToRun, jobOracle).flatMap { executionTupleOpt =>
      record(jobOracle, executionTupleOpt)
      
      logFinished(executionTupleOpt)
      
      finish(executionState)(executionTupleOpt)
    }
    
    Observables.merge(cancelledOrSkippedJobsObs, actuallyRunJobsObs)
  }

  private def emptyExecutionMap: Map[LJob, Execution] = Map.empty
  
  private def logFinished(jobs: Iterable[(LJob, Execution)]): Unit = {
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
  
  private def cancelJobs(jobsToCancel: Iterable[LJob]): Iterable[(LJob, Execution)] = {
    import loamstream.model.jobs.JobStatus.Canceled
    
    jobsToCancel.map(job => job -> Execution.from(job, Canceled, terminationReason = None))
  }
  
  private def handleSkipped(skippedJobs: Iterable[LJob]): Unit = {
    logSkipped(skippedJobs)
  }
  
  private def logJobsToBeRun(jobsToRun: Iterable[LJob]): Unit = {
    debug(s"Dispatching (${jobsToRun.size}) jobs to ChunkRunner:")
    
    jobsToRun.foreach(job => debug(s"Dispatching job to ChunkRunner: $job"))
  }
  
  private def logSkipped(skippedJobs: Iterable[LJob]): Unit = skippedJobs.size match {
    case 0 => debug("Skipped 0 jobs")
    case numSkipped => {
      info(s"Skipped ($numSkipped) jobs:")
    
      skippedJobs.foreach(job => info(s"Skipped: $job"))
    }
  }
  
  private def toSkippedResultTuples(skippedJobs: Iterable[LJob]): Iterable[(LJob, Execution)] = {
    skippedJobs.map(job => job -> Execution.from(job, JobStatus.Skipped, terminationReason = None))
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
    
    def scheduler: Scheduler = IOScheduler()
    
    private[RxExecuter] lazy val (executionContext, ecHandle) = {
      ExecutionContexts.threadPool(Defaults.maxNumConcurrentJobs)
    }
    
    private[RxExecuter] def chunkRunner: ChunkRunner = {
      AsyncLocalChunkRunner(Defaults.executionConfig, Defaults.maxNumConcurrentJobs)(Defaults.executionContext)
    }
  }
  
  def apply( //scalastyle:ignore parameter.number
    executionConfig: ExecutionConfig = Defaults.executionConfig,
    chunkRunner: ChunkRunner = Defaults.chunkRunner,
    fileMonitor: FileMonitor = Defaults.fileMonitor,
    windowLength: Duration = Defaults.windowLengthInSec,
    jobCanceler: JobCanceler = Defaults.jobCanceler,
    jobFilter: JobFilter = Defaults.jobFilter,
    executionRecorder: ExecutionRecorder = Defaults.executionRecorder,
    maxRunsPerJob: Int = Defaults.maxRunsPerJob,
    scheduler: Scheduler = Defaults.scheduler,
    terminableComponents: Iterable[Terminable] = Nil)
    (implicit executionContext: ExecutionContext = Defaults.executionContext): RxExecuter = {
      
    new RxExecuter(
      executionConfig,
      chunkRunner, 
      fileMonitor,
      windowLength, 
      jobCanceler,
      jobFilter, 
      executionRecorder,
      maxRunsPerJob, 
      scheduler,
      terminableComponents)(executionContext)
  }

  def default: RxExecuter = apply()
  
  def defaultWith(
      newJobFilter: JobFilter = Defaults.jobFilter, 
      newExecutionRecorder: ExecutionRecorder = Defaults.executionRecorder): RxExecuter = {
    
    RxExecuter(jobFilter = newJobFilter, executionRecorder = newExecutionRecorder)
  }

  /**
   * Turns the passed `runDataMap` into an observable that will fire once, producing a Map derived from `runDataMap`
   * by turning `runDataMap`'s values into Executions after waiting for any missing outputs. 
   */
  private[execute] def toExecutionMap(
      fileMonitor: FileMonitor)
      (runDataTuple: (LJob, RunData))
      (implicit context: ExecutionContext): Observable[(LJob, Execution)] = {
  
    def waitForOutputs(runData: RunData): Future[Execution] = {
      ExecuterHelpers.waitForOutputsAndMakeExecution(runData, fileMonitor)
    }
    
    val jobToExecutionFuture = {
      val (job, runData) = runDataTuple
    
      import Futures.Implicits._
      
      waitForOutputs(runData).map(execution => job -> execution)
    }
    
    Observable.from(jobToExecutionFuture)
  }
}
