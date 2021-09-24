package loamstream.drm.slurm

import org.scalatest.FunSuite
import loamstream.drm.SessionTracker
import loamstream.drm.DrmTaskId
import loamstream.conf.SlurmConfig
import loamstream.util.CommandInvoker
import scala.util.Try
import loamstream.util.RunResults
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.util.Success

/**
 * @author clint
 * May 25, 2021
 */
final class ScancelJobKillerTest extends FunSuite {
  test("makeTokens - no task arrays") {
    import ScancelJobKiller.makeTokens

    val actual = makeTokens(
      sessionTracker = SessionTracker.Noop,
      actualExecutable = "foo", 
      username = "bar")

    assert(actual === Seq("foo"))
  }

  test("makeTokens - some task arrays") {
    import ScancelJobKiller.makeTokens

    val sessionTracker = new SessionTracker.Default

    assert(sessionTracker.isEmpty)

    sessionTracker.register(Seq(DrmTaskId("blerg", 42), DrmTaskId("blerg", 43), DrmTaskId("zerg", 1)))

    assert(sessionTracker.isEmpty === false)

    //no task arrays
    val actual = makeTokens(
      sessionTracker = sessionTracker,
      actualExecutable = "foo", 
      username = "bar")

    assert(actual.take(4) === Seq("foo", "-u", "bar", "--quiet"))
    //NB: Use sets to ignore ordering in task-array-ids portion of params
    assert(actual.drop(4).toSet === Set("blerg", "zerg"))
  }

  test("killAllJobs") {
    def doTest(successExitCode: Int): Unit = {
      val sessionTracker = new SessionTracker.Default

      val taskArrayIds: Buffer[String] = new ArrayBuffer

      val commandInvoker: CommandInvoker.Sync[Unit] = new CommandInvoker.Sync[Unit] {
        override def apply(u: Unit): Try[RunResults.Completed] = {
          taskArrayIds ++= sessionTracker.taskArrayIdsSoFar

          Success(RunResults.Completed("foo", successExitCode, Nil, Nil))
        }
      }

      val jobKiller = new ScancelJobKiller(
        commandInvoker = commandInvoker, 
        sessionTracker = sessionTracker)

      assert(sessionTracker.isEmpty)

      assert(taskArrayIds === Nil)

      sessionTracker.register(Seq(DrmTaskId("blerg", 42), DrmTaskId("blerg", 43), DrmTaskId("zerg", 1)))

      assert(sessionTracker.isEmpty === false)

      jobKiller.killAllJobs()

      assert(taskArrayIds.toSet === Set("blerg", "zerg"))
    }

    doTest(0)
    doTest(1)
  }
}