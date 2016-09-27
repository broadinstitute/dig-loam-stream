package loamstream.model.execute

import loamstream.model.execute.RxExecuter.Tracker
import loamstream.model.jobs.{JobState, LJob, NoOpJob}
import loamstream.model.jobs.JobState.NotStarted
import loamstream.model.jobs.LJob._
import loamstream.util._
import rx.lang.scala.subjects.PublishSubject

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.concurrent.duration.Duration

/**
 * @author kaan
 *         date: Aug 17, 2016
 */
final case class RxExecuter(runner: ChunkRunner,
                            jobFilter: JobFilter,
                            tracker: Tracker = new Tracker)
                           (implicit executionContext: ExecutionContext) extends LExecuter with Loggable {

  private[this] val lock = new AnyRef

  // Mutable state variables
  val jobsAlreadyLaunched: ValueBox[Set[LJob]] = ValueBox(Set.empty)
  val jobStates: ValueBox[Map[LJob, JobState]] = ValueBox(Map.empty)
  val result: ValueBox[Map[LJob, Result]] = ValueBox(Map.empty)

  def flattenTree(tree: Set[LJob]): Set[LJob] = {
    tree.foldLeft(tree)((acc, x) =>
      x.inputs ++ flattenTree(x.inputs) ++ acc)
  }

  /** Check if jobs ready to be dispatched include a NoOpJob. If yes, make sure there is only one
   * and handle it by directly executing it
   */
  def checkForAndHandleNoOpJob(jobs: Set[LJob]): Unit = {
    if (!jobs.forall(!_.isInstanceOf[NoOpJob])) {
      trace("Handling NoOpJob")
      assert(jobs.size == 1, "There should be at most a single NoOpJob")
      val noOpJob = jobs.head
      val noOpResult = Await.result(noOpJob.execute, Duration.Inf)
      result.mutate(_ + (noOpJob -> noOpResult))
    }
  }

  def getJobsToBeDispatched(jobs: Set[LJob]): Set[LJob] =
    jobs.grouped(runner.maxNumJobs).toSet.headOption match {
      case Some(j) => j
      case _ => Set.empty[LJob]
    }

  def getRunnableJobsAndMarkThemAsLaunched(jobs: Set[LJob]): Set[LJob] = lock synchronized {
    trace("Jobs already launched: ")
    jobsAlreadyLaunched().foreach(job => trace(s"\tAlready launched: $job"))

    val allRunnableJobs = jobs.filter(_.isRunnable) -- jobsAlreadyLaunched()
    trace("Jobs available to run: ")
    allRunnableJobs.foreach(job => trace(s"\tAvailable to run: $job"))

    val jobsToDispatch = getJobsToBeDispatched(allRunnableJobs)

    // TODO: Remove when NoOpJob insertion into job ASTs is no longer necessary
    checkForAndHandleNoOpJob(jobsToDispatch)

    jobsAlreadyLaunched.mutate(_ ++ jobsToDispatch)

    jobsToDispatch
  }

  def filterOutAndProcessSkippableJobs(jobs: Set[LJob], filter: JobFilter): Set[LJob] = {
    val jobsToSkip = jobs.filterNot(filter.shouldRun)
    jobsToSkip foreach { job =>
      trace(s"\tBeing skipped: $job")
      job.updateAndEmitJobState(JobState.Skipped)
      result.mutate(_ + (job -> SkippedSuccess(job.name)))
    }

    jobs -- jobsToSkip
  }

  // scalastyle:off method.length
  def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    val allJobs = flattenTree(executable.jobs)

    val allJobStatuses = PublishSubject[Map[LJob, JobState]]

    def updateJobState(job: LJob, newState: JobState): Unit = {
      jobStates.mutate(_ + (job -> newState))
      allJobStatuses.onNext(jobStates())
    }

    // Future-Promise pair used as a flag to check if the main thread can be resumed (i.e. all jobs are done)
    val everythingIsDonePromise: Promise[Unit] = Promise()
    val everythingIsDoneFuture: Future[Unit] = everythingIsDonePromise.future

    def executeIter(): Unit = {
      debug("executeIter() is called...\n")

      val runnableJobs = getRunnableJobsAndMarkThemAsLaunched(allJobs)

      val jobs = filterOutAndProcessSkippableJobs(runnableJobs, jobFilter)
      trace("Jobs to dispatch now: ")
      jobs.foreach(job => trace(s"\tTo dispatch now: $job"))

      if (jobs.isEmpty) {
        if (jobStates().values.forall(_.isFinished)) {
          everythingIsDonePromise.trySuccess(())
        }
      } else {
        // TODO: Dispatch all job chunks so they are submitted without waiting for the next iteration
        import scala.concurrent.ExecutionContext.Implicits.global
        tracker.addJobs(jobs)
        for {
          newResultMap <- runner.run(jobs)(executionContext)
        } yield {
          result.mutate(_ ++ newResultMap)
          newResultMap.filter { case (job, result) => result.isSuccess }
                      .keys.foreach(job => jobFilter.record(job.outputs))
        }
      }
    }

    import scala.language.postfixOps
    allJobs foreach { job =>
      jobStates.mutate(_ + (job -> NotStarted))
      job.stateEmitter.subscribe(jobState => updateJobState(job, jobState))
    }

    allJobStatuses.sample(20 millis).subscribe(jobStatuses => executeIter())

    executeIter()

    // Block the main thread until all jobs are done
    Await.result(everythingIsDoneFuture, Duration.Inf)

    info("All jobs are done")
    import Maps.Implicits._
    result().strictMapValues(Hit(_))
  }
  // scalastyle:on method.length

  def clearStates(): Unit = {
    jobsAlreadyLaunched() = Set.empty
    result() = Map.empty
    jobStates() = Map.empty
  }
}

object RxExecuter {
  def apply(runner: ChunkRunner,
            tracker: Tracker)
           (implicit executionContext: ExecutionContext): RxExecuter = new RxExecuter(runner,
                                                                                      JobFilter.RunEverything,
                                                                                      tracker)

  def apply(runner: ChunkRunner)
           (implicit executionContext: ExecutionContext): RxExecuter = new RxExecuter(runner,
                                                                                      JobFilter.RunEverything,
                                                                                      new Tracker)

  def default: RxExecuter = defaultWith(JobFilter.RunEverything)

  def defaultWith(jobFilter: JobFilter): RxExecuter =
    new RxExecuter(AsyncLocalChunkRunner, jobFilter)(ExecutionContext.global)

  object AsyncLocalChunkRunner extends ChunkRunner {

    import ExecuterHelpers._

    override def maxNumJobs = 100 // scalastyle:ignore magic.number

    override def run(jobs: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, Result]] = {
      //NB: Use an iterator to evaluate input jobs lazily, so we can stop evaluating
      //on the first failure, like the old code did.
      val jobResultFutures = jobs.iterator.map(executeSingle)

      //NB: Convert the iterator to an IndexedSeq to force evaluation, and make sure
      //input jobs are evaluated before jobs that depend on them.
      val futureJobResults = Future.sequence(jobResultFutures).map(consumeUntilFirstFailure)

      futureJobResults.map(Maps.mergeMaps)
    }
  }

  final class Tracker() {
    private val executionSeq: ValueBox[Array[Set[LJob]]] = ValueBox(Array.empty)

    def addJobs(jobs: Set[LJob]): Unit = executionSeq.mutate(_ :+ jobs)

    def jobExecutionSeq = executionSeq.value
  }
}
