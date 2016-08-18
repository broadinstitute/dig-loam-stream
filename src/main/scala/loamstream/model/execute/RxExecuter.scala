package loamstream.model.execute

import loamstream.model.execute.RxExecuter.RxMockJob._
import loamstream.model.execute.RxExecuter.{RxMockExecutable, RxMockJob}
import loamstream.model.jobs.JobState
import loamstream.model.jobs.JobState.{Finished, NotStarted, Running}
import loamstream.util.{Hit, Loggable, Maps, Shot}
import rx.lang.scala.{Observable, Subscription}

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
 * @author kaan
 *         date: Aug 17, 2016
 */
final class RxExecuter extends Loggable {
  def hello(names: String*) {
    Observable.from(names) subscribe { n =>
      info(n)
    }
  }

  // scalastyle:off
  def execute(executable: RxMockExecutable)(implicit timeout: Duration = Duration.Inf): Map[RxMockJob, Shot[Result]] = {
    def flattenTree(tree: Set[RxMockJob]): Set[RxMockJob] = {
      tree.foldLeft(tree)((acc, x) =>
        x.inputs ++ flattenTree(x.inputs) ++ acc)
    }

    def getRunnableJobs(jobs: Set[RxMockJob]): Set[RxMockJob] = jobs.filter(_.isRunnable)

    val jobsAlreadyLaunched: collection.mutable.Set[RxMockJob] = collection.mutable.Set.empty
    var jobsReadyToDispatch: collection.mutable.Set[RxMockJob] = collection.mutable.Set.empty
    val jobsReadyToDispatchLock = new AnyRef
    var result: collection.mutable.Map[RxMockJob, Result] = collection.mutable.Map.empty
    val resultLock = new AnyRef

    def loop(jobs: Set[RxMockJob]): Unit = {
      if (jobs.nonEmpty) {
        jobsReadyToDispatchLock.synchronized {
          jobsReadyToDispatch =
            collection.mutable.Set[RxMockJob](getRunnableJobs(jobs).toArray: _*) -- jobsAlreadyLaunched
        }
        println("Jobs ready to dispatch: ")
        jobsReadyToDispatch.foreach(job => println("\t" + job.name))
        import scala.concurrent.ExecutionContext.Implicits.global
        Future {
          jobsReadyToDispatch.par.foreach { job =>
            val newResult = job -> job.execute
            resultLock.synchronized {
              result += newResult
            }
          }
        }

        Thread.sleep(2000)

        jobsReadyToDispatchLock.synchronized {
          jobsAlreadyLaunched ++= jobsReadyToDispatch
          loop(jobs -- jobsAlreadyLaunched.toSet)
        }
      }
    }

    val jobs = flattenTree(executable.jobs)
    jobs.foreach(_.deferredJobStateObservable.subscribe(jobState => println(jobState)))
    loop(jobs)

    import Maps.Implicits._
    resultLock.synchronized {
      result.toMap.strictMapValues(Hit(_))
    }
  }
  // scalastyle:on

  private def anyFailures(m: Map[RxMockJob, RxMockJob.Result]): Boolean = m.values.exists(_.isFailure)
}

object RxExecuter {
  def default: RxExecuter = new RxExecuter

  class RxMockJob(val name: String, val inputs: Set[RxMockJob] = Set.empty, delay: Int = 0)
    extends Loggable {

    def print(indent: Int = 0, doPrint: String => Unit = debug(_)): Unit = {
      val indentString = s"${"-" * indent} >"

      doPrint(s"$indentString ${this}")

      inputs.foreach(_.print(indent + 2))
    }

    private[this] val jobStatusLock = new AnyRef
    private[this] var _jobStatus: JobState = NotStarted
    def getJobState = jobStatusLock.synchronized(_jobStatus)
    def setJobState(newState: JobState) = jobStatusLock.synchronized(_jobStatus = newState)
    def emitJobState: Subscription = {
      Observable.just(getJobState) subscribe { js =>
        getJobState
      }
    }
    def jobStateObservable: Observable[JobState] = Observable.just(getJobState)
    def deferredJobStateObservable: Observable[JobState] = Observable.defer(jobStateObservable)

    def isRunnable: Boolean = getJobState == NotStarted && (inputs.isEmpty || inputs.forall(_.getJobState == Finished))

    private[this] val executionCountLock = new AnyRef
    private[this] var _executionCount = 0
    def executionCount = executionCountLock.synchronized(_executionCount)

    def execute: Result = {
      debug("\t\tStarting to execute job: " + this.name)
      setJobState(Running)
      Thread.sleep(delay)
      setJobState(Finished)
      executionCountLock.synchronized(_executionCount += 1)
      debug("\t\t\tFinished executing job: " + this.name)
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

}