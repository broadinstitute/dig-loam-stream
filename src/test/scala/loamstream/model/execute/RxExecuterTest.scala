package loamstream.model.execute

import loamstream.model.execute.RxExecuter.{RxMockExecutable, RxMockJob}
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: Aug 17, 2016
 */
final class RxExecuterTest extends FunSuite {
  private val executer = RxExecuter.default

  private def addNoOp(executable: RxMockExecutable): RxMockExecutable =
    RxMockExecutable(Set(new RxMockJob("NoOp", executable.jobs)))

  private def executionCount(job: RxMockJob): Int = job.asInstanceOf[RxMockJob].executionCount

  // scalastyle:off
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

    val job11 = new RxMockJob("Job_1_1")
    val job12 = new RxMockJob("Job_1_2")
    val job21 = new RxMockJob("Job_2_1", Set(job11))
    val job22 = new RxMockJob("Job_2_2", Set(job11))
    val job23 = new RxMockJob("Job_2_3", Set(job12))
    val job24 = new RxMockJob("Job_2_4", Set(job12))
    val job31 = new RxMockJob("Job_3_1", Set(job21, job22))
    val job32 = new RxMockJob("Job_3_2", Set(job23, job24))
    val job4 = new RxMockJob("Job_4", Set(job31, job32))

    assert(job11.executionCount == 0)
    assert(job12.executionCount == 0)
    assert(job21.executionCount == 0)
    assert(job22.executionCount == 0)
    assert(job23.executionCount == 0)
    assert(job24.executionCount == 0)
    assert(job31.executionCount == 0)
    assert(job32.executionCount == 0)
    assert(job4.executionCount == 0)

    val executable = RxMockExecutable(Set(job4))
    val result = executer.execute(executable)

    assert(job11.executionCount == 1)
    assert(job12.executionCount == 1)
    assert(job21.executionCount == 1)
    assert(job22.executionCount == 1)
    assert(job23.executionCount == 1)
    assert(job24.executionCount == 1)
    assert(job31.executionCount == 1)
    assert(job32.executionCount == 1)
    assert(job4.executionCount == 1)

    assert(result.size === 9)
  }
  // scalastyle:off
}
