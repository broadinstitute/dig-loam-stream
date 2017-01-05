package loamstream.model.execute

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob
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
      _ = logJobs(executable)
      jobs <- chunk.to[Set]
      if jobs.nonEmpty
      (jobsToRun, skippedJobs) = jobs.partition(jobFilter.shouldRun)
      _ = debug(s"SKIPPING (${skippedJobs.size}) $skippedJobs")
      _ = debug(s"RUNNING (${jobsToRun.size}) $jobsToRun")
      _ = debug(s"Dispatching jobs to ChunkRunner: $jobsToRun")
      _ = markJobsSkipped(skippedJobs)
      resultMap <- runner.run(jobsToRun)
      _ = record(resultMap)
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
  
  private def logJobs(executable: Executable): Unit = {
    def log(s: String) = debug(s)
      
    executable.jobs.head.print(doPrint = log, header = Some("Current Job Statuses:"))
  }
  
  private def markJobsSkipped(skippedJobs: Set[LJob]): Unit = {
    skippedJobs.foreach(_.updateAndEmitJobState(JobState.Skipped))
  }
  
  private def toSkippedResultMap(skippedJobs: Set[LJob]): Map[LJob, JobState] = {
    import Traversables.Implicits._
      
    skippedJobs.mapTo(job => JobState.Skipped)
  }
  
  private def record(newResultMap: Map[LJob, JobState]): Unit = {
    val executions = newResultMap.map { case (job, jobState) => Execution(jobState, job.outputs) }
        
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
  
  
