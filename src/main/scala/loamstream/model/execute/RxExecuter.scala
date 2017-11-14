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

/**
 * @author kaan
 * @author clint
 *         date: Aug 17, 2016
 */
final case class RxExecuter(
    runner: ChunkRunner,
    windowLength: Duration,
    jobFilter: JobFilter,
    maxRunsPerJob: Int)(implicit val executionContext: ExecutionContext) extends Executer with Loggable {
  
  require(maxRunsPerJob >= 1, s"The maximum number of times to run each job must not be negative; got $maxRunsPerJob")
  
  override def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Execution] = {
    import loamstream.util.ObservableEnrichments._
    
    val ioScheduler: Scheduler = IOScheduler()

    //An Observable stream of jobs runs; each job is emitted when it becomes runnable.  This can be because the
    //job's dependencies finished successfully, or because the job failed and we've decided to restart it.
    //Note the use of `distinct`.  It's brute force, but simplifies the logic here and in LJob for the case where
    //multiple 'root' jobs depend on the same upstream job.  In this case, without `distinct`, the upstream job
    //would be run twice.
    val runnables: Observable[JobRun] = Observables.merge(executable.jobs.toSeq.map(_.runnables)).distinct
    
    //An observable stream of "chunks" of runnable jobs, with each chunk represented as an observable stream.
    //Jobs are buffered up until the amount of time indicated by 'windowLength' elapses, or 'runner.maxNumJobs'
    //are collected.  When that happens, the buffered "chunk" of jobs is emitted.
    val chunks: Observable[Observable[JobRun]] = runnables.tumbling(windowLength, runner.maxNumJobs, ioScheduler)
    
    val chunkResults: Observable[Map[LJob, Execution]] = for {
      chunk <- chunks
      _ = logJobForest(executable)
      //NB: .to[Set] is important: jobs in a chunk should be distinct, 
      //so they're not run more than once before transitioning to a terminal state.
      jobs <- chunk.to[Set]
      //NB: Filter out jobs from this chunk that finished when run as part of another chunk, so we don't run them
      //more times than necessary.  This helps in the face of job-restarting, since we can't call `distinct()` 
      //on `runnables` and declare victory like we did before, since that would filter out restarting jobs that 
      //already ran 
      (finishedJobs, notFinishedJobs) = jobs.partition(_.status.isTerminal)
      if notFinishedJobs.nonEmpty
      (jobsToRun, skippedJobs) = notFinishedJobs.map(_.job).partition(jobFilter.shouldRun)
      _ = handleSkippedJobs(skippedJobs)
      executionMap <- runJobs(jobsToRun)
      _ = record(executionMap)
      _ = logFinishedJobs(executionMap)
      skippedResultMap = toSkippedResultMap(skippedJobs)
    } yield {
      executionMap ++ skippedResultMap
    }
   
    
    //NB: We no longer stop on the first failure, but run each sub-tree of jobs as far as possible.
    //TODO: Make this configurable
    val futureMergedResults = chunkResults.to[Seq].map(Maps.mergeMaps).firstAsFuture
    
    Await.result(futureMergedResults, timeout)
  }
  
  private def logFinishedJobs(jobs: Map[LJob, Execution]): Unit = {
    for {
      (job, execution) <- jobs
      status = execution.status
    } {
      info(s"Finished with $status when running $job")
    }
  }
  
  //NB: shouldRestart() mostly factored out to the companion object for simpler testing
  private def shouldRestart(job: LJob): Boolean = RxExecuter.shouldRestart(job, maxRunsPerJob)
  
  private def runJobs(jobsToRun: Set[LJob]): Observable[Map[LJob, Execution]] = {
    logJobsToBeRun(jobsToRun)
    
    runner.run(jobsToRun, shouldRestart)
  }
  
  private def handleSkippedJobs(skippedJobs: Set[LJob]): Unit = {
    logSkippedJobs(skippedJobs)
    
    markJobsSkipped(skippedJobs)
  }
  
  private def logJobForest(executable: Executable): Unit = {
    def log(printingJob: JobNode)(s: String) = debug(s)
      
    executable.jobs.head.print(doPrint = log, header = Some("Current Job Statuses:"))
  }
  
  private def logJobsToBeRun(jobsToRun: Set[LJob]): Unit = {
    debug(s"Dispatching (${jobsToRun.size}) jobs to ChunkRunner:")
    
    jobsToRun.foreach(job => debug(s"Dispatching job to ChunkRunner: $job"))
  }
  
  private def logSkippedJobs(skippedJobs: Set[LJob]): Unit = skippedJobs.size match {
    case 0 => debug("Skipped 0 jobs")
    case numSkipped => {
      info(s"Skipped ($numSkipped) jobs:")
    
      skippedJobs.foreach(job => info(s"Skipped: $job"))
    }
  }
  
  private def markJobsSkipped(skippedJobs: Set[LJob]): Unit = {
    skippedJobs.foreach(_.transitionTo(JobStatus.Skipped))
  }
  
  private def toSkippedResultMap(skippedJobs: Set[LJob]): Map[LJob, Execution] = {
    import loamstream.util.Traversables.Implicits._
      
    skippedJobs.mapTo(job => Execution.from(job, JobStatus.Skipped))
  }

  private def record(executionMap: Map[LJob, Execution]): Unit = {
    jobFilter.record(executionMap.values)
  }
}

object RxExecuter extends Loggable {
  object Defaults {
    val maxNumConcurrentJobs: Int = 8
    
    //NB: Use a short windowLength to speed up tests
    val windowLength: Double = 0.1
    val windowLengthInSec: Duration = windowLength.seconds
  
    val jobFilter = JobFilter.RunEverything
  
    val maxRunsPerJob: Int = 4
  }
  
  def apply(runner: ChunkRunner)(implicit executionContext: ExecutionContext): RxExecuter = {
    new RxExecuter(runner, Defaults.windowLengthInSec, Defaults.jobFilter, Defaults.maxRunsPerJob)
  }

  def default: RxExecuter = {
    implicit val executionContext = ExecutionContext.global

    val chunkRunner = AsyncLocalChunkRunner(Defaults.maxNumConcurrentJobs)

    new RxExecuter(chunkRunner, Defaults.windowLengthInSec, Defaults.jobFilter, Defaults.maxRunsPerJob)
  }
  
  def defaultWith(newJobFilter: JobFilter): RxExecuter = {
    val d = default
    
    d.copy(jobFilter = newJobFilter)(d.executionContext)
  }
  
  private[execute] def shouldRestart(job: LJob, maxRunsPerJob: Int): Boolean = {
    val runCount = job.runCount
    
    val result = runCount < maxRunsPerJob
    
    debug(s"Restarting $job ? $result (job has run $runCount times, max is $maxRunsPerJob)")
    
    result
  }
}
