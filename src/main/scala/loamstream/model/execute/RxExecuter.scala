package loamstream.model.execute

import loamstream.model.execute.RxExecuter.RxMockJob.{Result, SimpleSuccess}
import loamstream.model.execute.RxExecuter.{RxMockExecutable, RxMockJob}
import loamstream.util.{Hit, Loggable, Maps, Shot}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
 * @author clint
 *         date: Jun 1, 2016
 */
final class RxExecuter(implicit executionContext: ExecutionContext) {

  def execute(executable: RxMockExecutable)(implicit timeout: Duration = Duration.Inf):
  Map[RxMockJob, Shot[Result]] = {
    import Maps.Implicits._
    val futureResults = Future.sequence(executable.jobs.map(executeJob)).map(Maps.mergeMaps)

    val future = futureResults.strictMapValues(Hit(_))

    Await.result(future, timeout)
  }

  private def executeJob(job: RxMockJob)(implicit executionContext: ExecutionContext):
  Future[Map[RxMockJob, Result]] = {
    def flattenTree(tree: Set[RxMockJob]): Set[RxMockJob] = {
      tree.foldLeft(tree)((acc, x) =>
        x.inputs ++ flattenTree(x.inputs) ++ acc)
    }

    def getRunnableJobs(jobs: Set[RxMockJob]): Set[RxMockJob] = ???

    def loop(remainingOption: Option[Set[RxMockJob]], acc: Map[RxMockJob, Result]): Future[Map[RxMockJob, Result]] = {
      remainingOption match {
        case None => Future.successful(acc)
        case Some(jobs) =>
          val shouldStop = jobs.isEmpty
          val jobsReadyToDispatch = getRunnableJobs(jobs)
          val results = runner.run(jobsReadyToDispatch)
          val next = if (shouldStop) None else Some(jobs -- jobsReadyToDispatch)
          loop(next, acc ++ results)
      }
    }

    val jobs = flattenTree(Set(job))

  }

  private def anyFailures(m: Map[RxMockJob, RxMockJob.Result]): Boolean = m.values.exists(_.isFailure)
}

object RxExecuter {

  def default: RxExecuter = new RxExecuter

  class RxMockJob(name: String, val inputs: Set[RxMockJob] = Set.empty, delay: Int = 0) extends Loggable {
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


    def execute(implicit context: ExecutionContext): Future[Result] = {
      lock.synchronized(_executionCount += 1)
      Thread.sleep(delay)
      isSuccessful() = true
      Future.successful(RxMockJob.SimpleSuccess(name))
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

  final case class RxNoOpJob(name: String = "NoOpJob", override val inputs: Set[RxMockJob] = Set.empty)
    extends RxMockJob(name, inputs) {

    override def execute(implicit context: ExecutionContext): Future[Result] =
      Future.successful(SimpleSuccess(name))
  }

  final case class RxMockExecutable(jobs: Set[RxMockJob]) {
    def ++(oExecutable: RxMockExecutable): RxMockExecutable = RxMockExecutable(jobs ++ oExecutable.jobs)

    def addNoOpRootJob: RxMockExecutable = RxMockExecutable(Set(RxNoOpJob(inputs = jobs)))
  }

  object RxMockExecutable {
    val empty = RxMockExecutable(Set.empty)
  }

}