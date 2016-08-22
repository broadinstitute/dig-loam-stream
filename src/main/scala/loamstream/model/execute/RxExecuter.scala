package loamstream.model.execute

import java.lang.Boolean

import loamstream.model.execute.RxExecuter.RxMockJob._
import loamstream.model.execute.RxExecuter.{RxMockExecutable, RxMockJob, Tracker}
import loamstream.model.jobs.JobState
import loamstream.model.jobs.JobState.{NotStarted, Running, Succeeded}
import loamstream.util._
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.Observable

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
 * @author kaan
 *         date: Aug 17, 2016
 */
final class RxExecuter(val tracker: Tracker) extends Loggable {
  // scalastyle:off method.length
  def execute(executable: RxMockExecutable)(implicit timeout: Duration = Duration.Inf): Map[RxMockJob, Shot[Result]] = {
    def flattenTree(tree: Set[RxMockJob]): Set[RxMockJob] = {
      tree.foldLeft(tree)((acc, x) =>
        x.inputs ++ flattenTree(x.inputs) ++ acc)
    }

    def getRunnableJobs(jobs: Set[RxMockJob]): Set[RxMockJob] = jobs.filter(_.isRunnable)

    val _jobsAlreadyLaunched = collection.mutable.Set.empty[RxMockJob]
    val jobsAlreadyLaunchedLock = new AnyRef
    def jobsAlreadyLaunched = jobsAlreadyLaunchedLock.synchronized(_jobsAlreadyLaunched)

    var _jobsReadyToDispatch = collection.mutable.Set.empty[RxMockJob]
    val jobsReadyToDispatchLock = new AnyRef
    def jobsReadyToDispatch = jobsReadyToDispatchLock.synchronized(_jobsReadyToDispatch)

    val _jobStates = collection.mutable.Map.empty[RxMockJob, JobState]
    val jobStatesLock = new AnyRef

    var _result = collection.mutable.Map.empty[RxMockJob, Result]
    val resultLock = new AnyRef

    val everythingIsDonePromise: Promise[Unit] = Promise()
    val everythingIsDoneFuture: Future[Unit] = everythingIsDonePromise.future

    val allJobStatuses = PublishSubject[Map[RxMockJob, JobState]]

    def updateJobState(job: RxMockJob, newState: JobState): Unit = {
      jobStatesLock.synchronized {
        _jobStates(job) = newState
      }
      trace("+++Emitting all job statuses")
      allJobStatuses.onNext(_jobStates.toMap)
    }

    def executeIter(jobs: Set[RxMockJob]): Unit = {
      if (jobs.isEmpty) {
        if (_jobStates.values.forall(_ == Succeeded)) {
          everythingIsDonePromise.success(())
        }
      } else {
        trace("Jobs already launched: ")
        jobsAlreadyLaunched.foreach(job => trace("\t" + job.name))

        jobsReadyToDispatchLock.synchronized {
          _jobsReadyToDispatch =
            collection.mutable.Set[RxMockJob](getRunnableJobs(jobs).toArray: _*) -- jobsAlreadyLaunched
        }
        debug("Jobs ready to dispatch: ")
        jobsReadyToDispatch.foreach(job => debug("\t" + job.name))
        import scala.concurrent.ExecutionContext.Implicits.global
        Future {
          tracker.addJobs(jobsReadyToDispatch.toSet)
          jobsReadyToDispatch.par.foreach { job =>
            val newResult = job -> job.execute
            resultLock.synchronized(_result += newResult)
            jobsReadyToDispatchLock.synchronized(_jobsAlreadyLaunched += job)
          }
        }
      }
    }

    val jobs = flattenTree(executable.jobs)

    import scala.language.postfixOps
    jobs foreach { job =>
      jobStatesLock.synchronized {
        _jobStates += job -> NotStarted
      }
      job.jobStateChange.subscribe(jobState => updateJobState(job, jobState))
    }

    allJobStatuses.sample(10 millis).subscribe(jobStatuses => {
      executeIter(getRunnableJobs(jobStatuses.keySet))
    })

    executeIter(jobs)

    Await.result(everythingIsDoneFuture, Duration.Inf)

    import Maps.Implicits._
    resultLock.synchronized {
      _result.toMap.strictMapValues(Hit(_))
    }
  }

  // scalastyle:off method.length

  private def anyFailures(m: Map[RxMockJob, RxMockJob.Result]): Boolean = m.values.exists(_.isFailure)
}

object RxExecuter {
  def default: RxExecuter = new RxExecuter(new Tracker)

  class RxMockJob(val name: String, val inputs: Set[RxMockJob] = Set.empty,
                  val dependencies: Set[RxMockJob] = Set.empty, delay: Int = 0) extends Loggable {

    def print(indent: Int = 0, doPrint: String => Unit = debug(_)): Unit = {
      val indentString = s"${"-" * indent} >"

      doPrint(s"$indentString ${this}")

      inputs.foreach(_.print(indent + 2))
    }

    private[this] val jobStatusLock = new AnyRef
    private[this] var _jobStatus: JobState = NotStarted

    def getJobState = jobStatusLock.synchronized(_jobStatus)

    def setJobState(newState: JobState) = jobStatusLock.synchronized(_jobStatus = newState)

    def jobStateObservable: Observable[JobState] = Observable.just(getJobState)

    val jobStateChange = PublishSubject[JobState]

    private def emitJobState: Unit = {
      trace(s"***Emitting state change for $name ==> " + getJobState)
      jobStateChange.onNext(getJobState)
    }

    def dependenciesDone: Boolean = dependencies.isEmpty || dependencies.forall(_.getJobState == JobState.Succeeded)

    def inputsDone: Boolean = inputs.isEmpty || inputs.forall(_.getJobState == Succeeded)

    def isRunnable: Boolean = getJobState == NotStarted && dependenciesDone && inputsDone


    private[this] val executionCountLock = new AnyRef
    private[this] var _executionCount = 0

    def executionCount = executionCountLock.synchronized(_executionCount)

    def execute: Result = {
      trace("\t\tStarting job: " + this.name)
      setJobState(Running)
      emitJobState
      Thread.sleep(delay)
      trace("\t\t\tFinishing job: " + this.name)
      setJobState(Succeeded)
      emitJobState
      executionCountLock.synchronized(_executionCount += 1)
      RxMockJob.SimpleSuccess(name)
    }
  }

  object RxMockJob {

    sealed trait Result {
      def isSuccess: Boolean

      def isFailure: Boolean

      def message: String
    }

    object Result {
      def attempt(f: => Result): Result = {
        try {
          f
        } catch {
          case NonFatal(ex) => FailureFromThrowable(ex)
        }
      }
    }

    trait Success extends Result {
      final def isSuccess: Boolean = true

      final def isFailure: Boolean = false

      def successMessage: String

      def message: String = s"Success! $successMessage"
    }

    final case class SimpleSuccess(successMessage: String) extends Success

    trait Failure extends Result {
      final def isSuccess: Boolean = false

      final def isFailure: Boolean = true

      def failureMessage: String

      def message: String = s"Failure! $failureMessage"
    }

    final case class SimpleFailure(failureMessage: String) extends Failure

    final case class FailureFromThrowable(cause: Throwable) extends Failure {
      def failureMessage: String = cause.getMessage
    }

  }

  final case class RxNoOpJob(override val name: String = "NoOpJob", override val inputs: Set[RxMockJob] = Set.empty)
    extends RxMockJob(name, inputs) {

    override def execute: Result = SimpleSuccess(name)
  }

  final case class RxMockExecutable(jobs: Set[RxMockJob]) {
    def ++(oExecutable: RxMockExecutable): RxMockExecutable = RxMockExecutable(jobs ++ oExecutable.jobs)

    def addNoOpRootJob: RxMockExecutable = RxMockExecutable(Set(RxNoOpJob(inputs = jobs)))
  }

  object RxMockExecutable {
    val empty = RxMockExecutable(Set.empty)
  }

  final case class Tracker() {
    private var _executionSeq = collection.mutable.ArrayBuffer.empty[Set[RxMockJob]]
    private val executionSeqLock = new AnyRef

    def addJobs(jobs: Set[RxMockJob]): Unit = executionSeqLock.synchronized(_executionSeq += jobs)

    def jobExecutionSeq = executionSeqLock.synchronized(_executionSeq.toArray)
  }

}