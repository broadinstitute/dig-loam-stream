package loamstream.model.execute

import loamstream.conf.UgerConfig
import loamstream.model.execute.RxExecuter.AsyncLocalChunkRunner
import loamstream.model.execute.RxExecuterTest.MockChunkRunner
import loamstream.model.jobs.{LJob, RxMockJob}
import loamstream.model.jobs.LJob.Result
import loamstream.util.Loggable
import org.scalatest.FunSuite

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * @author kyuksel
 *         date: Aug 17, 2016
 */
final class RxExecuterTest extends FunSuite with Loggable {
  // scalastyle:off magic.number
  test("New leaves are executed as soon as possible when there is no delay") {
    /* A four-step pipeline:
     *
     *           Job21
     *          /      \
     * Job11 --          -- Job31
     *          \      /         \
     *           Job22            \
     *                              -- Job4
     *           Job23            /
     *          /      \         /
     * Job12 --          -- Job32
     *          \      /
     *           Job24
     */

    val executer = RxExecuter.default

    val job11 = RxMockJob("Job_1_1")
    val job12 = RxMockJob("Job_1_2")
    val job21 = RxMockJob("Job_2_1", Set(job11))
    val job22 = RxMockJob("Job_2_2", Set(job11))
    val job23 = RxMockJob("Job_2_3", Set(job12))
    val job24 = RxMockJob("Job_2_4", Set(job12))
    val job31 = RxMockJob("Job_3_1", Set(job21, job22))
    val job32 = RxMockJob("Job_3_2", Set(job23, job24))
    val job4 = RxMockJob("Job_4", Set(job31, job32))

    assert(job11.executionCount === 0)
    assert(job12.executionCount === 0)
    assert(job21.executionCount === 0)
    assert(job22.executionCount === 0)
    assert(job23.executionCount === 0)
    assert(job24.executionCount === 0)
    assert(job31.executionCount === 0)
    assert(job32.executionCount === 0)
    assert(job4.executionCount === 0)

    val executable = LExecutable(Set(job4))
    val result = executer.execute(executable)

    assert(job11.executionCount === 1)
    assert(job12.executionCount === 1)
    assert(job21.executionCount === 1)
    assert(job22.executionCount === 1)
    assert(job23.executionCount === 1)
    assert(job24.executionCount === 1)
    assert(job31.executionCount === 1)
    assert(job32.executionCount === 1)
    assert(job4.executionCount === 1)

    assert(result.size === 9)

    // Check if jobs were correctly chunked
    val jobExecutionSeq = executer.tracker.jobExecutionSeq
    assert(jobExecutionSeq.length === 4, jobExecutionSeq.foreach(seq => debug(seq.toString)))
    assert(jobExecutionSeq(0) === Set(job11, job12))
    assert(jobExecutionSeq(1) === Set(job21, job22, job23, job24))
    assert(jobExecutionSeq(2) === Set(job31, job32))
    assert(jobExecutionSeq(3) === Set(job4))
  }

  test("New leaves are executed as soon as possible when initial jobs don't start simultaneously") {
    /* A four-step pipeline:
     *
     *           Job21
     *          /      \
     * Job11 --          -- Job31
     *          \      /         \
     *           Job22            \
     *                              -- Job4
     *           Job23            /
     *          /      \         /
     * Job12 --          -- Job32
     *          \      /
     *           Job24
     */

    val executer = RxExecuter.default

    // The delay added to job11 should cause job23 and job24 to be bundled and executed prior to job21 and job22
    lazy val job11 = RxMockJob("Job_1_1", dependencies = Set(job12))
    lazy val job12 = RxMockJob("Job_1_2")
    lazy val job21 = RxMockJob("Job_2_1", Set(job11))
    lazy val job22 = RxMockJob("Job_2_2", Set(job11))
    lazy val job23 = RxMockJob("Job_2_3", Set(job12), dependencies = Set(job31))
    lazy val job24 = RxMockJob("Job_2_4", Set(job12))
    lazy val job31 = RxMockJob("Job_3_1", Set(job21, job22))
    lazy val job32 = RxMockJob("Job_3_2", Set(job23, job24))
    lazy val job4 = RxMockJob("Job_4", Set(job31, job32))

    assert(job11.executionCount === 0)
    assert(job12.executionCount === 0)
    assert(job21.executionCount === 0)
    assert(job22.executionCount === 0)
    assert(job23.executionCount === 0)
    assert(job24.executionCount === 0)
    assert(job31.executionCount === 0)
    assert(job32.executionCount === 0)
    assert(job4.executionCount === 0)

    val executable = LExecutable(Set(job4))
    val result = executer.execute(executable)

    assert(job11.executionCount === 1)
    assert(job12.executionCount === 1)
    assert(job21.executionCount === 1)
    assert(job22.executionCount === 1)
    assert(job23.executionCount === 1)
    assert(job24.executionCount === 1)
    assert(job31.executionCount === 1)
    assert(job32.executionCount === 1)
    assert(job4.executionCount === 1)

    assert(result.size === 9)

    // Check if jobs were correctly chunked
    val jobExecutionSeq = executer.tracker.jobExecutionSeq
    assert(jobExecutionSeq.length === 7)
    assert(jobExecutionSeq(0) === Set(job12))
    assert(jobExecutionSeq(1) === Set(job11, job24))
    assert(jobExecutionSeq(2) === Set(job21, job22))
    assert(jobExecutionSeq(3) === Set(job31))
    assert(jobExecutionSeq(4) === Set(job23))
    assert(jobExecutionSeq(5) === Set(job32))
    assert(jobExecutionSeq(6) === Set(job4))
  }

  test("maxNumJobs is taken into account") {
    /* A four-step pipeline:
     *
     *           Job21
     *          /      \
     * Job11 --          -- Job31
     *          \      /         \
     *           Job22            \
     *                              -- Job4
     *           Job23            /
     *          /      \         /
     * Job12 --  Job24 - -- Job32
     *          \      /
     *           Job25
     */

    val job11 = RxMockJob("Job_1_1")
    val job12 = RxMockJob("Job_1_2")
    val job21 = RxMockJob("Job_2_1", Set(job11))
    val job22 = RxMockJob("Job_2_2", Set(job11))
    val job23 = RxMockJob("Job_2_3", Set(job12))
    val job24 = RxMockJob("Job_2_4", Set(job12))
    val job25 = RxMockJob("Job_2_5", Set(job12))
    val job31 = RxMockJob("Job_3_1", Set(job21, job22))
    val job32 = RxMockJob("Job_3_2", Set(job23, job24, job25))
    val job4 = RxMockJob("Job_4", Set(job31, job32))

    val executable = LExecutable(Set(job4))

    val maxSimultaneousJobs = 4
    val mockRunner = MockChunkRunner(AsyncLocalChunkRunner, maxSimultaneousJobs)
    val executer = RxExecuter(mockRunner)

    executer.execute(executable)

    val chunks = mockRunner.chunks
    val expectedMaxSimultaneousJobs = 4
    val expectedNumberOfChunks = 5
    assert(chunks.size === expectedNumberOfChunks)
    assert(chunks.forall(_.size <= expectedMaxSimultaneousJobs))
  }
  // scalastyle:on magic.number
}

object RxExecuterTest {
  private final case class MockChunkRunner(delegate: ChunkRunner, maxNumJobs: Int) extends ChunkRunner {
    var chunks: Seq[Set[LJob]] = Nil

    override def run(leaves: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, Result]] = {
      chunks = chunks :+ leaves

      delegate.run(leaves)
    }
  }

  private object MockChunkRunner {
    def apply(delegate: ChunkRunner): MockChunkRunner = new MockChunkRunner(delegate,
      UgerConfig.fromFile("src/test/resources/loamstream-test.conf").get.ugerMaxNumJobs)
  }
}
