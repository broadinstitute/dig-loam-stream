package loamstream.model.execute

import loamstream.model.execute.RxExecuter.RxMockJob.{Result, SimpleSuccess}
import loamstream.model.execute.RxExecuter.{RxMockExecutable, RxMockJob}
import loamstream.util.{Hit, Loggable, Maps, Shot}
import rx.Ctx

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
 * @author kaan
 *         date: Aug 8, 2016
 */
final class RxExecuter {
  implicit val ctx: Ctx.Owner = Ctx.Owner.safe

  def execute(executable: RxMockExecutable)(implicit timeout: Duration = Duration.Inf): Map[RxMockJob, Shot[Result]] = {
    def flattenTree(tree: Set[RxMockJob]): Set[RxMockJob] = {
      tree.foldLeft(tree)((acc, x) =>
        x.inputs ++ flattenTree(x.inputs) ++ acc)
    }

    def getRunnableJobs(jobs: Set[RxMockJob]): Set[RxMockJob] = jobs.filter(_.isRunnable.now)

    def loop(remainingOption: Option[Set[RxMockJob]], acc: Map[RxMockJob, Result]): Map[RxMockJob, Result] = {
      remainingOption match {
        case None => acc
        case Some(jobs) =>
          val shouldStop = jobs.isEmpty
          val jobsReadyToDispatch = getRunnableJobs(jobs)
          println("Jobs read to dispatch: ") // scalastyle:ignore
          jobsReadyToDispatch.foreach(job => println(job.name)) // scalastyle:ignore
          //val results = jobsReadyToDispatch.par.map(job => (job, job.execute)).toMap
          val results = jobsReadyToDispatch.par.map(job => (job, job.execute)).toMap
          val next = if (shouldStop) None else Some(jobs -- jobsReadyToDispatch)
          Thread.sleep(1000) // scalastyle:ignore magic.number
          loop(next, acc ++ results)
      }
    }

    val jobs = flattenTree(executable.jobs)
    val results = loop(Option(jobs), Map.empty)

    import Maps.Implicits._
    results.strictMapValues(Hit(_))
  }

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

    final def isRunnable(implicit ctx: Ctx.Owner): Rx[Boolean] = Rx {
      inputs.isEmpty || dependenciesSuccessful.now
    }

    final val isSuccessful: Var[Boolean] = Var(false)

    final def dependenciesSuccessful(implicit ctx: Ctx.Owner): Rx[Boolean] = Rx {
      inputs.forall(_.isSuccessful())
    }

    private[this] val lock = new AnyRef

    private[this] var _executionCount = 0

    def executionCount = lock.synchronized(_executionCount)

    def execute: Result = {
      lock.synchronized(_executionCount += 1)
      println("\tStarting to execute job: " + this.name)
      Thread.sleep(delay)
      isSuccessful() = true
      println("\t\tFinished executing job: " + this.name)
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