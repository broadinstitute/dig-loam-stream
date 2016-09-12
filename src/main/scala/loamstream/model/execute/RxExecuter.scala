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
final case class RxExecuter(runner: ChunkRunner, tracker: Tracker = Tracker())
                           (implicit executionContext: ExecutionContext)
  extends LExecuter with Loggable {
  // scalastyle:off method.length
  def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    def flattenTree(tree: Set[LJob]): Set[LJob] = {
      tree.foldLeft(tree)((acc, x) =>
        x.inputs ++ flattenTree(x.inputs) ++ acc)
    }

    def getRunnableJobs(jobs: Set[LJob]): Set[LJob] = jobs.filter(_.isRunnable)

    // Mutable state variables
    val jobsAlreadyLaunched: ValueBox[Set[LJob]] = ValueBox(Set.empty)
    val jobsReadyToDispatch: ValueBox[Set[LJob]] = ValueBox(Set.empty)
    val jobStates: ValueBox[Map[LJob, JobState]] = ValueBox(Map.empty)
    val result: ValueBox[Map[LJob, Result]] = ValueBox(Map.empty)

    def jobsToBeDispatched: Set[LJob] = jobsReadyToDispatch().grouped(runner.maxNumJobs).toSet.headOption match {
      case Some(j) => j
      case _ => Set.empty[LJob]
    }

    val allJobStatuses = PublishSubject[Map[LJob, JobState]]

    def updateJobState(job: LJob, newState: JobState): Unit = {
      jobStates.mutate(_ + (job -> newState))
      allJobStatuses.onNext(jobStates())
    }

    /** Check if jobs ready to be dispatched include a NoOpJob. If yes, make sure there is only one
     * and handle it by directly executing it
     */
    def checkForAndHandleNoOpJob(): Unit = {
      if (!jobsToBeDispatched.forall(!_.isInstanceOf[NoOpJob])) {
        trace("Handling NoOpJob")
        assert(jobsToBeDispatched.size == 1, "There should be at most a single NoOpJob")
        val noOpJob = jobsToBeDispatched.head
        val noOpResult = Await.result(noOpJob.execute, Duration.Inf)
        result.mutate(_ + (noOpJob -> noOpResult))
        jobsAlreadyLaunched.mutate(_ + noOpJob)
      }
    }

    // Future-Promise pair used as a flag to check if the main thread can be resumed (i.e. all jobs are done)
    val everythingIsDonePromise: Promise[Unit] = Promise()
    val everythingIsDoneFuture: Future[Unit] = everythingIsDonePromise.future

    def executeIter(jobs: Set[LJob]): Unit = {
      trace("executeIter() is called...")
      if (jobs.isEmpty) {
        if (jobStates().values.forall(_.isFinished)) {
          everythingIsDonePromise.trySuccess(())
        }
      } else {
        trace("Jobs already launched: ")
        jobsAlreadyLaunched().foreach(job => trace("\t" + job))

        jobsReadyToDispatch() = getRunnableJobs(jobs) -- jobsAlreadyLaunched()
        // TODO: Consider moving updating of jobsAlreadyLaunched here. It may prevent race conditions, which is
        // TODO: probably the culprit for sporadic test failures

        trace("Jobs ready to dispatch: ")
        jobsReadyToDispatch().foreach(job => debug("\t" + job))

        debug("Jobs to be dispatched at this time: ")
        jobsToBeDispatched.foreach(job => debug("\t" + job))

        // TODO: Remove when NoOpJob insertion into job ASTs is no longer necessary
        checkForAndHandleNoOpJob()

        // TODO: Dispatch all job chunks so they are submitted without waiting for the next iteration
        import scala.concurrent.ExecutionContext.Implicits.global
        Future {
          tracker.addJobs(jobsToBeDispatched)
          val newResultMap = Await.result(runner.run(jobsToBeDispatched), Duration.Inf)
          result.mutate(_ ++ newResultMap)
          jobsAlreadyLaunched.mutate(_ ++ jobsToBeDispatched)
        }
      }
    }

    val jobs = flattenTree(executable.jobs)

    import scala.language.postfixOps
    jobs foreach { job =>
      jobStates.mutate(_ + (job -> NotStarted))
      job.stateEmitter.subscribe(jobState => updateJobState(job, jobState))
    }

    allJobStatuses.sample(20 millis).subscribe(
      jobStatuses => {
        executeIter(getRunnableJobs(jobStatuses.keySet))
      }
    )

    executeIter(jobs)

    // Block the main thread until all jobs are done
    Await.result(everythingIsDoneFuture, Duration.Inf)

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

  final case class Tracker() {
    private val executionSeq: ValueBox[Array[Set[LJob]]] = ValueBox(Array.empty)

    def addJobs(jobs: Set[LJob]): Unit = executionSeq.mutate(_ :+ jobs)

    def jobExecutionSeq = executionSeq.value
  }

}