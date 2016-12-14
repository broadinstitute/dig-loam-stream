package loamstream.model.execute

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobState
import loamstream.model.jobs.JobState.NotStarted
import loamstream.model.jobs.LJob
import loamstream.model.jobs.NoOpJob
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.Maps
import loamstream.util.ValueBox
import rx.lang.scala.subjects.PublishSubject

/**
 * @author kaan
 *         date: Aug 17, 2016
 */
final case class RxExecuter(runner: ChunkRunner,
                            jobFilter: JobFilter)
                           (implicit val executionContext: ExecutionContext) extends Executer with Loggable {

  private[this] val lock = new AnyRef

  // Mutable state variables
  private val jobsAlreadyLaunched: ValueBox[Set[LJob]] = ValueBox(Set.empty)
  private val jobStates: ValueBox[Map[LJob, JobState]] = ValueBox(Map.empty)
  private val result: ValueBox[Map[LJob, JobState]] = ValueBox(Map.empty)

  /** Check if jobs ready to be dispatched include a NoOpJob. If yes, make sure there is only one
   * and handle it by directly executing it
   */
  private def checkForAndHandleNoOpJob(jobsToDispatch: Set[LJob]): Unit = {
    def isNoOpJob(job: LJob): Boolean = job.isInstanceOf[NoOpJob] 
    
    if (jobsToDispatch.exists(isNoOpJob)) {
      trace("Handling NoOpJob")

      assert(jobsToDispatch.size == 1, "There should be at most a single NoOpJob")
      
      val noOpJob = jobsToDispatch.head
      val noOpResult = Futures.waitFor(noOpJob.execute)
      
      result.mutate(_ + (noOpJob -> noOpResult))
    }
  }

  private def getJobsToBeDispatched(jobs: Set[LJob]): Set[LJob] = {
    val firstChunk = jobs.grouped(runner.maxNumJobs).toSeq.headOption
    
    firstChunk.getOrElse(Set.empty)
  }

  private def getRunnableJobsAndMarkThemAsLaunched(jobs: Set[LJob]): Set[LJob] = lock.synchronized {
    trace("Jobs already launched: ")
    jobsAlreadyLaunched().foreach(job => trace(s"\tAlready launched: $job"))

    val allRunnableJobs = jobs.filter(_.isRunnable)
    
    val unlaunchedRunnableJobs = allRunnableJobs -- jobsAlreadyLaunched()
    trace("Jobs available to run: ")
    unlaunchedRunnableJobs.foreach(job => trace(s"\tAvailable to run: $job"))

    val jobsToDispatch = getJobsToBeDispatched(unlaunchedRunnableJobs)

    // TODO: Remove when NoOpJob insertion into job ASTs is no longer necessary
    checkForAndHandleNoOpJob(jobsToDispatch)

    jobsAlreadyLaunched.mutate(_ ++ jobsToDispatch)

    jobsToDispatch
  }

  private def filterOutAndProcessSkippableJobs(jobs: Set[LJob], filter: JobFilter): Set[LJob] = {
    debug(s"filtering jobs: $jobs")
    
    val jobsToSkip = jobs.filterNot(filter.shouldRun)
    
    debug(s"Skipping ${jobsToSkip.size} jobs:")
    
    jobsToSkip.foreach { job =>
      debug(s"\tSkipped: $job")
      job.updateAndEmitJobState(JobState.Skipped)
      result.mutate(_ + (job -> JobState.Skipped))
    }

    jobs -- jobsToSkip
  }

  private def allJobsAreFinished: Boolean = {
    val jobsToStates = jobStates()
    
    val allFinished = jobsToStates.keys.forall(_.isFinished)
    
    if(!allFinished && isDebugEnabled) {
      val notFinishedOption = jobsToStates.find { case (_, state) => !state.isFinished }
    
      notFinishedOption match {
        case Some((notFinishedJob, state)) => 
          debug(s"All jobs are NOT finished; still running ($state): $notFinishedJob")
        case None => ()
      }
    }
    
    allFinished
  }
  
  private def executeIter(allJobs: Set[LJob], everythingIsDonePromise: Promise[Unit]): Future[Unit] = {
    debug("executeIter() is called...\n")
    
    val runnableJobs = getRunnableJobsAndMarkThemAsLaunched(allJobs)

    val jobsToDispatch = filterOutAndProcessSkippableJobs(runnableJobs, jobFilter)
    
    debug("Now running these jobs: ")
    jobsToDispatch.foreach(job => debug(s"\tRunning now: $job"))
    
    if (jobsToDispatch.isEmpty && allJobsAreFinished) {
      
      everythingIsDonePromise.trySuccess(())
        
      Future.successful(())
    } else {
      // TODO: Dispatch all job chunks so they are submitted without waiting for the next iteration
      for {
        newResultMap <- runner.run(jobsToDispatch)(executionContext)
        _ = debug(s"Results from ChunkRunner: $newResultMap")
        unit <- recordChunkExecution(newResultMap)
      } yield {
        unit
      }
    }
  }
  
  private def recordChunkExecution(newResultMap: Map[LJob, JobState]): Future[Unit] = {
        
    result.mutate(_ ++ newResultMap)
        
    val executions = newResultMap.map { case (job, jobState) => Execution(jobState, job.outputs.map(_.toOutputRecord)) }
        
    debug(s"Recording Executions (${executions.size}): $executions")
    
    Future.successful(jobFilter.record(executions))
  }
  
  override def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, JobState] = {
    //NB: Clear out our state, to make sure that state built up from a previous invocation of execute() won't
    //interfere with this one. :/
    clearStates()
    
    val allJobs = ExecuterHelpers.flattenTree(executable.jobs)
    
    trace(s"All Jobs: $allJobs")

    val allJobStatuses = PublishSubject[Map[LJob, JobState]]

    def updateJobState(job: LJob, newState: JobState): Unit = {
      jobStates.mutate(_ + (job -> newState))
      
      allJobStatuses.onNext(jobStates())
    }

    // Future-Promise pair used as a flag to check if the main thread can be resumed (i.e. all jobs are done)
    val everythingIsDonePromise: Promise[Unit] = Promise()
    val everythingIsDoneFuture: Future[Unit] = everythingIsDonePromise.future

    allJobs foreach { job =>
      jobStates.mutate(_ + (job -> NotStarted))
      job.stateEmitter.subscribe(jobState => updateJobState(job, jobState))
    }

    //NB: Block waiting for executeIter :(
    def doExecuteIter() = Futures.waitFor(executeIter(allJobs, everythingIsDonePromise))
    
    allJobStatuses.sample(20.millis).subscribe(_ => doExecuteIter())
    
    doExecuteIter()
    
    // Block the main thread until all jobs are done
    Futures.waitFor(everythingIsDoneFuture)

    info("All jobs are done")
    
    result()
  }

  private def clearStates(): Unit = {
    jobsAlreadyLaunched() = Set.empty
    result() = Map.empty
    jobStates() = Map.empty
  }
}

object RxExecuter {
  def apply(runner: ChunkRunner)(implicit executionContext: ExecutionContext): RxExecuter = {
    new RxExecuter(runner, JobFilter.RunEverything)
  }

  def default: RxExecuter = defaultWith(JobFilter.RunEverything)

  def defaultWith(jobFilter: JobFilter): RxExecuter = {
    new RxExecuter(AsyncLocalChunkRunner, jobFilter)(ExecutionContext.global)
  }
  
  object AsyncLocalChunkRunner extends ChunkRunner {

    import ExecuterHelpers._

    override def maxNumJobs = 100 // scalastyle:ignore magic.number

    override def run(jobs: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, JobState]] = {
      //NB: Use an iterator to evaluate input jobs lazily, so we can stop evaluating
      //on the first failure, like the old code did.
      val jobResultFutures = jobs.iterator.map(executeSingle)

      //NB: Convert the iterator to an IndexedSeq to force evaluation, and make sure
      //input jobs are evaluated before jobs that depend on them.
      val futureJobResults = Future.sequence(jobResultFutures).map(consumeUntilFirstFailure)

      futureJobResults.map(Maps.mergeMaps)
    }
  }
}
