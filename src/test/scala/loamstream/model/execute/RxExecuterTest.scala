package loamstream.model.execute

import loamstream.model.execute.RxExecuter.{RxMockExecutable, RxMockJob}

/**
 * @author clint
 *         date: Jun 2, 2016
 * @author kyuksel
 *         date: Jul 15, 2016
 */
final class RxExecuterTest extends ExecuterTest {
  def makeExecuter: RxExecuter = RxExecuter.default

  private val executer = RxExecuter.default

  private def addNoOp(executable: RxMockExecutable): RxMockExecutable =
    RxMockExecutable(Set(new RxMockJob("NoOp", executable.jobs)))

  private def executionCount(job: RxMockJob): Int = job.asInstanceOf[RxMockJob].executionCount

  ignore("New leaves are executed as soon as possible") {
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
    // The delay added to job11 should cause job23 and job24 to be bundled and executed prior to job21 and job22
    val job11 = new RxMockJob("1st_step_job_1", delay = 1000) // scalastyle:ignore magic.number
    val job12 = new RxMockJob("1st_step_job_2")
    val job21 = new RxMockJob("2nd_step_job_1", Set(job11))
    val job22 = new RxMockJob("2nd_step_job_2", Set(job11))
    val job23 = new RxMockJob("2nd_step_job_3", Set(job12))
    val job24 = new RxMockJob("2nd_step_job_4", Set(job12))
    val job31 = new RxMockJob("3rd_step_job_1", Set(job21, job22))
    val job32 = new RxMockJob("3rd_step_job_2", Set(job23, job24))
    val job4 = new RxMockJob("4th_step_job", Set(job31, job32))

    val executable = RxMockExecutable(Set(job4))
    val result = executer.execute(executable)
    assert(true)
  }
}
