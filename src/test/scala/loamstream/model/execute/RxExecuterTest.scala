package loamstream.model.execute

import loamstream.model.execute.RxExecuter.{RxMockExecutable, RxMockJob}
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: Aug 17, 2016
 */
final class RxExecuterTest extends FunSuite {
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

    val job11 = new RxMockJob("Job_1_1")
    val job12 = new RxMockJob("Job_1_2")
    val job21 = new RxMockJob("Job_2_1", Set(job11))
    val job22 = new RxMockJob("Job_2_2", Set(job11))
    val job23 = new RxMockJob("Job_2_3", Set(job12))
    val job24 = new RxMockJob("Job_2_4", Set(job12))
    val job31 = new RxMockJob("Job_3_1", Set(job21, job22))
    val job32 = new RxMockJob("Job_3_2", Set(job23, job24))
    val job4 = new RxMockJob("Job_4", Set(job31, job32))

    assert(job11.executionCount === 0)
    assert(job12.executionCount === 0)
    assert(job21.executionCount === 0)
    assert(job22.executionCount === 0)
    assert(job23.executionCount === 0)
    assert(job24.executionCount === 0)
    assert(job31.executionCount === 0)
    assert(job32.executionCount === 0)
    assert(job4.executionCount === 0)

    val executable = RxMockExecutable(Set(job4))
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
    assert(jobExecutionSeq.length === 4)
    assert(jobExecutionSeq(0) === Set(job11, job12))
    assert(jobExecutionSeq(1) === Set(job21, job22, job23, job24))
    assert(jobExecutionSeq(2) === Set(job31, job32))
    assert(jobExecutionSeq(3) === Set(job4))
  }

  test("New leaves are executed as soon as possible when there are delays") {
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
    val job11 = new RxMockJob("Job_1_1", delay = 100)
    val job12 = new RxMockJob("Job_1_2", delay = 50)
    val job21 = new RxMockJob("Job_2_1", Set(job11))
    val job22 = new RxMockJob("Job_2_2", Set(job11))
    val job23 = new RxMockJob("Job_2_3", Set(job12))
    val job24 = new RxMockJob("Job_2_4", Set(job12), delay = 200)
    val job31 = new RxMockJob("Job_3_1", Set(job21, job22), delay = 300)
    val job32 = new RxMockJob("Job_3_2", Set(job23, job24))
    val job4 = new RxMockJob("Job_4", Set(job31, job32))

    assert(job11.executionCount === 0)
    assert(job12.executionCount === 0)
    assert(job21.executionCount === 0)
    assert(job22.executionCount === 0)
    assert(job23.executionCount === 0)
    assert(job24.executionCount === 0)
    assert(job31.executionCount === 0)
    assert(job32.executionCount === 0)
    assert(job4.executionCount === 0)

    val executable = RxMockExecutable(Set(job4))
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
    assert(jobExecutionSeq.length === 6)
    assert(jobExecutionSeq(0) === Set(job11, job12))
    assert(jobExecutionSeq(1) === Set(job23, job24))
    assert(jobExecutionSeq(2) === Set(job21, job22))
    assert(jobExecutionSeq(3) === Set(job31))
    assert(jobExecutionSeq(4) === Set(job32))
    assert(jobExecutionSeq(5) === Set(job4))
  }
  // scalastyle:on magic.number
}
