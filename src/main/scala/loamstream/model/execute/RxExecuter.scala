package loamstream.model.execute

import loamstream.model.execute.RxExecuter.Tracker
import loamstream.model.jobs.{JobState, LJob, NoOpJob, Output}
import loamstream.model.jobs.JobState.{NotStarted, Running, Succeeded}
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
final case class RxExecuter(runner: ChunkRunner, tracker: Tracker = new Tracker)
                           (implicit executionContext: ExecutionContext) extends LExecuter with Loggable {
  // scalastyle:off method.length
  private[this] val lock = new AnyRef

  // Mutable state variables
  val jobsAlreadyLaunched: ValueBox[Set[LJob]] = ValueBox(Set.empty)
  val jobStates: ValueBox[Map[LJob, JobState]] = ValueBox(Map.empty)
  val result: ValueBox[Map[LJob, Result]] = ValueBox(Map.empty)

  def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
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

    val allJobs = flattenTree(executable.jobs)

    def getRunnableJobsAndMarkThemAsLaunched(): Set[LJob] = lock synchronized {
      debug("Jobs already launched: ")
      jobsAlreadyLaunched().foreach(job => debug(s"\tAlready launched: $job"))

      val allRunnableJobs = allJobs.filter(_.isRunnable) -- jobsAlreadyLaunched()
      debug("Jobs available to run: ")
      allRunnableJobs.foreach(job => debug(s"\tAvailable to run: $job"))

      val jobsToDispatch = getJobsToBeDispatched(allRunnableJobs)
      debug("Jobs to dispatch now: ")
      jobsToDispatch.foreach(job => debug(s"\tTo dispatch now: $job"))

      // TODO: Remove when NoOpJob insertion into job ASTs is no longer necessary
      checkForAndHandleNoOpJob(jobsToDispatch)

      jobsAlreadyLaunched.mutate(_ ++ jobsToDispatch)

      jobsToDispatch
    }

    def getJobsToBeDispatched(jobs: Set[LJob]): Set[LJob] =
      jobs.grouped(runner.maxNumJobs).toSet.headOption match {
        case Some(j) => j
        case _ => Set.empty[LJob]
      }

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

      val jobs = getRunnableJobsAndMarkThemAsLaunched()
      if (jobs.isEmpty) {
        if (jobStates().values.forall(_.isFinished)) {
          everythingIsDonePromise.trySuccess(())
        }
      } else {
        // TODO: Dispatch all job chunks so they are submitted without waiting for the next iteration
        import scala.concurrent.ExecutionContext.Implicits.global
        Future {
          tracker.addJobs(jobs)
          val newResultMap = Await.result(runner.run(jobs), Duration.Inf)
          result.mutate(_ ++ newResultMap)
        }
      }
    }

    import scala.language.postfixOps
    allJobs foreach { job =>
      jobStates.mutate(_ + (job -> NotStarted))
      job.stateEmitter.subscribe(jobState => updateJobState(job, jobState))
    }

    allJobStatuses.sample(20 millis).subscribe(
      jobStatuses => {
        executeIter()
      }
    )

    executeIter()

    // Block the main thread until all jobs are done
    Await.result(everythingIsDoneFuture, Duration.Inf)

    info("All jobs are done")
    import Maps.Implicits._
    result().strictMapValues(Hit(_))
  }

  // scalastyle:off method.length
}

object RxExecuter {
  def default: RxExecuter = new RxExecuter(AsyncLocalChunkRunner)(ExecutionContext.global)

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

  class RxMockJob(override val name: String, val inputs: Set[LJob] = Set.empty, val outputs: Set[Output] = Set.empty,
                  override val dependencies: Set[LJob] = Set.empty, delay: Int = 0) extends LJob {

    private[this] val count = ValueBox(0)

    def executionCount = count.value

    def execute(implicit context: ExecutionContext): Future[Result] = Future {
      trace("\t\tStarting job: " + this.name)
      updateAndEmitJobState(Running)
      if (delay > 0) {
        Thread.sleep(delay)
      }
      trace("\t\t\tFinishing job: " + this.name)
      updateAndEmitJobState(Succeeded)
      count.mutate(_ + 1)
      LJob.SimpleSuccess(name)
    }

    def copy(
              name: String = this.name,
              inputs: Set[LJob] = this.inputs,
              outputs: Set[Output] = this.outputs,
              dependencies: Set[LJob] = this.dependencies,
              delay: Int = this.delay): RxMockJob = new RxMockJob(name, inputs, outputs, dependencies, delay)

    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    override def toString: String = name
  }

  final class Tracker() {
    private val executionSeq: ValueBox[Array[Set[LJob]]] = ValueBox(Array.empty)

    def addJobs(jobs: Set[LJob]): Unit = executionSeq.mutate(_ :+ jobs)

    def jobExecutionSeq = executionSeq.value
  }

}