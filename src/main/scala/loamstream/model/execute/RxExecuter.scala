package loamstream.model.execute

import loamstream.model.execute.RxExecuter.RxMockJob.{Result, SimpleSuccess}
import loamstream.model.execute.RxExecuter.{RxMockExecutable, RxMockJob}
import loamstream.util.{Hit, Loggable, Maps, Shot}
import rx.{Ctx, Rx, Var}

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
 * @author kaan
 *         date: Aug 8, 2016
 */
final class RxExecuter {
  implicit val ctx: Ctx.Owner = Ctx.Owner.safe

  // scalastyle:off
  def execute(executable: RxMockExecutable)(implicit timeout: Duration = Duration.Inf): Map[RxMockJob, Shot[Result]] = {
    def flattenTree(tree: Set[RxMockJob]): Set[RxMockJob] = {
      tree.foldLeft(tree)((acc, x) =>
        x.inputs ++ flattenTree(x.inputs) ++ acc)
    }

    def getRunnableJobs(jobs: Set[RxMockJob]): Set[RxMockJob] = jobs.filter(_.isRunnable.now)

    def getRunnableJobsMap(jobs: Set[RxMockJob]): Rx[Map[RxMockJob, Rx[Boolean]]] = Rx {
      jobs.map(job => (job, job.isRunnable)).toMap
    }
/*
    def loop(remainingOption: Option[Set[RxMockJob]], result: collection.mutable.Map[RxMockJob, Result]):
    Map[RxMockJob, Result] = {
      remainingOption match {
        case None => Map.empty
        case Some(jobs) =>
          val shouldStop = jobs.isEmpty
          val jobsReadyToDispatch = getRunnableJobs(jobs)
          println("Jobs ready to dispatch: ")
          jobsReadyToDispatch.foreach(job => println("\t" + job.name))
          import scala.concurrent.ExecutionContext.Implicits.global
          Future { jobsReadyToDispatch.par.foreach(job => result += job -> job.execute) }
          val next = if (shouldStop) None else Some(jobs.filterNot(_.isSuccessful.now))
          Thread.sleep(500) // scalastyle:ignore magic.number
          loop(next, result)
      }
    }
*/

    val jobs = flattenTree(executable.jobs)
    val result: collection.mutable.Map[RxMockJob, Result] = collection.mutable.Map.empty
    val jobStatuses = Rx { jobs.map(_.isRunnable()) }

    val observer = jobStatuses.trigger {
      val jobsReadyToDispatch = getRunnableJobs(jobs)
      println("Jobs ready to dispatch: ")
      jobsReadyToDispatch.foreach(job => println("\t" + job.name))
      import scala.concurrent.ExecutionContext.Implicits.global
      Future { jobsReadyToDispatch.par.foreach(job => result += job -> job.execute) }
    }

    //loop(Option(jobs), result)

    //if (!jobs.forall(_.isSuccessful.now)) execute(executable)

    Thread.sleep(10000)

    import Maps.Implicits._
    result.toMap.strictMapValues(Hit(_))
  }
  // scalastyle:on

  private def anyFailures(m: Map[RxMockJob, RxMockJob.Result]): Boolean = m.values.exists(_.isFailure)
}

object RxExecuter {
  def default: RxExecuter = new RxExecuter

  class RxMockJob(val name: String, val inputs: Set[RxMockJob] = Set.empty, delay: Int = 0) extends Loggable {
    def print(indent: Int = 0, doPrint: String => Unit = debug(_)): Unit = {
      val indentString = s"${"-" * indent} >"

      doPrint(s"$indentString ${this}")

      inputs.foreach(_.print(indent + 2))
    }

    import rx._
/*
    final def isRunnable(implicit ctx: Ctx.Owner): Rx[Boolean] = Rx {
      inputs.isEmpty || (!isSuccessful() && !isRunning() && dependenciesSuccessful)
    }
*/
    final val isSuccessful: Var[Boolean] = Var(false)
    final val isRunning: Var[Boolean] = Var(false)

    implicit val ctxOwner: Ctx.Owner = Ctx.Owner.safe
    val isRunnable: Rx[Boolean] = Rx {
      !isSuccessful() && !isRunning() && (inputs.isEmpty || inputs.forall(_.isSuccessful()))
    }

    private[this] val lock = new AnyRef

    private[this] var _executionCount = 0

    def executionCount = lock.synchronized(_executionCount)

    // scalastyle:off regex
    def execute: Result = {
      lock.synchronized(_executionCount += 1)
      println("\t\tStarting to execute job: " + this.name)
      isRunning() = true
      Thread.sleep(delay)
      isSuccessful() = true
      println("\t\t\tFinished executing job: " + this.name)
      RxMockJob.SimpleSuccess(name)
    }
    // scalastyle:on regex
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

    override def execute: Result =
      SimpleSuccess(name)
  }

  final case class RxMockExecutable(jobs: Set[RxMockJob]) {
    def ++(oExecutable: RxMockExecutable): RxMockExecutable = RxMockExecutable(jobs ++ oExecutable.jobs)

    def addNoOpRootJob: RxMockExecutable = RxMockExecutable(Set(RxNoOpJob(inputs = jobs)))
  }

  object RxMockExecutable {
    val empty = RxMockExecutable(Set.empty)
  }

}