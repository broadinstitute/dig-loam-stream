package loamstream.model.execute

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import loamstream.model.jobs.{Execution, JobStatus, LJob}
import loamstream.util.Loggable
import loamstream.util.Maps
import loamstream.util.Observables
import rx.lang.scala.Observable
import rx.lang.scala.Scheduler
import rx.lang.scala.schedulers.IOScheduler
import loamstream.util.Traversables

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
  
  require(maxRunsPerJob >= 0, s"The maximum number of times to run each job must not be negative; got $maxRunsPerJob")
  
  override def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Execution] = {
    import loamstream.util.ObservableEnrichments._
    
    //An Observable stream of jobs; each job is emitted when it becomes runnable.
    //Note the use of 'distinct' to avoid running jobs more than once, if that job is depended on by multiple 'root' 
    //jobs in an Executable.  This is a bit brute-force, but allows for simpler logic in LJob.
    val runnables: Observable[LJob] = {
      Observables.merge(executable.jobs.toSeq.map(_.runnables))
    }
    
    val ioScheduler: Scheduler = IOScheduler()
    
    //An observable stream of "chunks" of runnable jobs, with each chunk represented as an observable stream.
    //Jobs are buffered up until the amount of time indicated by 'windowLength' elapses, or 'runner.maxNumJobs'
    //are collected.  When that happens, the buffered "chunk" of jobs is emitted.
    val chunks: Observable[Observable[LJob]] = runnables.tumbling(windowLength, runner.maxNumJobs, ioScheduler)
    
    val chunkResults: Observable[Map[LJob, Execution]] = for {
      chunk <- chunks
      _ = logJobForest(executable)
      jobs <- chunk.to[Set]
      if jobs.nonEmpty
      (jobsToRun, skippedJobs) = jobs.partition(jobFilter.shouldRun)
      _ = handleSkippedJobs(skippedJobs)
      executionMap <- runJobs(jobsToRun)
      _ = record(executionMap)
      _ = logFinishedJobs(executionMap)
      skippedResultMap = toSkippedResultMap(skippedJobs)
    } yield {
      executionMap ++ skippedResultMap
    }
   
    import ExecuterHelpers.anyFailures
    
    //NB: Sanity check, which fixes failures with the kinship step.
    //Emit result maps up to - and including! - the first one that contains a failure, then stop. 
    //val chunkResultsUpToFirstFailure = chunkResults.takeUntil(anyFailures(_))
    
    //Collect the results from each chunk, and merge them, producing a future holding the merged results
    //val futureMergedResults = chunkResultsUpToFirstFailure.to[Seq].map(Maps.mergeMaps).firstAsFuture
    
    val futureMergedResults = chunkResults.to[Seq].map(Maps.mergeMaps).firstAsFuture
    
    Await.result(futureMergedResults, timeout)
  }
  
  def logFinishedJobs(jobs: Map[LJob, Execution]): Unit = {
    for {
      (job, execution) <- jobs
      status = execution.status
    } {
      info(s"Finished with $status when running $job")
    }
  }
  
  private def shouldRestart(job: LJob): Boolean = RxExecuter.shouldRestart(job, maxRunsPerJob)
  
  def runJobs(jobsToRun: Set[LJob]): Observable[Map[LJob, Execution]] = {
    logJobsToBeRun(jobsToRun)
    
    runner.run(jobsToRun, shouldRestart)
  }
  
  private def handleSkippedJobs(skippedJobs: Set[LJob]): Unit = {
    logSkippedJobs(skippedJobs)
    
    markJobsSkipped(skippedJobs)
  }
  
  private def logJobForest(executable: Executable): Unit = {
    def log(s: String) = debug(s)
      
    executable.jobs.head.print(doPrint = log, header = Some("Current Job Statuses:"))
  }
  
  private def logJobsToBeRun(jobsToRun: Set[LJob]): Unit = {
    debug(s"Dispatching (${jobsToRun.size}) jobs to ChunkRunner:")
    
    jobsToRun.foreach(job => debug(s"Dispatching job to ChunkRunner: $job"))
  }
  
  private def logSkippedJobs(skippedJobs: Set[LJob]): Unit = {
    info(s"Skipping (${skippedJobs.size}) jobs:")
    
    skippedJobs.foreach(job => info(s"Skipped: $job"))
  }
  
  private def markJobsSkipped(skippedJobs: Set[LJob]): Unit = {
    skippedJobs.foreach(_.transitionTo(JobStatus.Skipped))
  }
  
  private def toSkippedResultMap(skippedJobs: Set[LJob]): Map[LJob, Execution] = {
    import Traversables.Implicits._
      
    skippedJobs.mapTo(job => Execution.from(job, JobStatus.Skipped))
  }

  private def record(executionMap: Map[LJob, Execution]): Unit = {
    debug(s"Recording ${executionMap.size} Execution(s): $executionMap")
    jobFilter.record(executionMap.values)
  }
}

object RxExecuter extends Loggable {
  object Defaults {
    val maxNumConcurrentJobs: Int = 8
    
    //NB: Use a short windowLength to speed up tests
    val windowLength: Double = 0.25
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
  
  //TODO: TEST
  private[execute] def shouldRestart(job: LJob, maxRunsPerJob: Int): Boolean = job.runCount < maxRunsPerJob
}
  
  
