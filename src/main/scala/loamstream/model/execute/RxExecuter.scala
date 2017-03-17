package loamstream.model.execute

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob
import loamstream.uger.Queue
import loamstream.util.Loggable
import loamstream.util.Maps
import loamstream.util.Observables
import rx.lang.scala.Observable
import rx.lang.scala.Scheduler
import rx.lang.scala.schedulers.IOScheduler
import loamstream.util.Traversables
import loamstream.model.execute.Resources.LocalResources

/**
 * @author kaan
 * @author clint
 *         date: Aug 17, 2016
 */
final case class RxExecuter(
    runner: ChunkRunner,
    windowLength: Duration,
    jobFilter: JobFilter)(implicit val executionContext: ExecutionContext) extends Executer with Loggable {
  
  override def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, JobState] = {
    import loamstream.util.ObservableEnrichments._
    
    //An Observable stream of jobs; each job is emitted when it becomes runnable.
    //Note the use of 'distinct' to avoid running jobs more than once, if that job is depended on by multiple 'root' 
    //jobs in an LExecutable.  This is a bit brute-force, but allows for simpler logic in LJob.
    val runnables: Observable[LJob] = {
      Observables.merge(executable.jobs.toSeq.map(_.runnables)).distinct
    }
    
    val ioScheduler: Scheduler = IOScheduler()
    
    //An observable stream of "chunks" of runnable jobs, with each chunk represented as an observable stream.
    //Jobs are buffered up until the amount of time indicated by 'windowLength' elapses, or 'runner.maxNumJobs'
    //are collected.  When that happens, the buffered "chunk" of jobs is emitted.
    val chunks: Observable[Observable[LJob]] = runnables.tumbling(windowLength, runner.maxNumJobs, ioScheduler)
    
    val chunkResults: Observable[Map[LJob, JobState]] = for {
      chunk <- chunks
      _ = logJobForest(executable)
      jobs <- chunk.to[Set]
      if jobs.nonEmpty
      (jobsToRun, skippedJobs) = jobs.partition(jobFilter.shouldRun)
      _ = handleSkippedJobs(skippedJobs)
      resultMap <- runJobs(jobsToRun)
      _ = record(resultMap)
      _ = logFinishedJobs(resultMap)
      skippedResultMap = toSkippedResultMap(skippedJobs)
    } yield {
      resultMap ++ skippedResultMap
    }
   
    import ExecuterHelpers.anyFailures
    
    //NB: Sanity check, which fixes failures with the kinship step.
    //Emit result maps up to - and including! - the first one that contains a failure, then stop. 
    val chunkResultsUpToFirstFailure = chunkResults.takeUntil(anyFailures(_))
    
    //Collect the results from each chunk, and merge them, producing a future holding the merged results
    val futureMergedResults = chunkResultsUpToFirstFailure.to[Seq].map(Maps.mergeMaps).firstAsFuture
    
    Await.result(futureMergedResults, timeout)
  }
  
  def logFinishedJobs(jobs: Map[LJob, JobState]): Unit = {
    for {
      (job, state) <- jobs
    } {
      info(s"Finished with $state when running $job")
    }
  }
  
  def runJobs(jobsToRun: Set[LJob]): Observable[Map[LJob, JobState]] = {
    logJobsToBeRun(jobsToRun)
    
    runner.run(jobsToRun)
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
    skippedJobs.foreach(_.updateAndEmitJobState(JobState.Skipped))
  }
  
  private def toSkippedResultMap(skippedJobs: Set[LJob]): Map[LJob, JobState] = {
    import Traversables.Implicits._
      
    skippedJobs.mapTo(job => JobState.Skipped)
  }
  
  private def record(newResultMap: Map[LJob, JobState]): Unit = {
    val executions = newResultMap.map { case (job, jobState) =>
      // TODO Replace the placeholders for `env/settings/resources` objects put in place to get the code to compile
      Execution(ExecutionEnvironment.Local, "REPLACE_ME", LocalSettings(), LocalResources.DUMMY,
        jobState, job.outputs.map(_.toOutputRecord)) }

    debug(s"Recording Executions (${executions.size}): $executions")
    
    jobFilter.record(executions)
  }
}

object RxExecuter {
  // scalastyle:off magic.number
  
  val defaultMaxNumConcurrentJobs = 8
  
  //NB: Use a short windowLength to speed up tests
  val defaultWindowLength = 0.25.seconds
  
  // scalastyle:on magic.number
  
  val defaultJobFilter = JobFilter.RunEverything
  
  def apply(runner: ChunkRunner)(implicit executionContext: ExecutionContext): RxExecuter = {
    new RxExecuter(runner, defaultWindowLength, defaultJobFilter)
  }
  
  def default: RxExecuter = {
    implicit val executionContext = ExecutionContext.global
  
    val chunkRunner = AsyncLocalChunkRunner(defaultMaxNumConcurrentJobs)
    
    new RxExecuter(chunkRunner, defaultWindowLength, defaultJobFilter)
  }
  
  def defaultWith(newJobFilter: JobFilter): RxExecuter = {
    val d = default
    
    d.copy(jobFilter = newJobFilter)(d.executionContext)
  }
}
  
  
