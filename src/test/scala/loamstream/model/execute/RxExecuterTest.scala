package loamstream.model.execute

import loamstream.conf.UgerConfig
import loamstream.model.execute.RxExecuter.AsyncLocalChunkRunner

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result

/**
 * @author clint
 *         date: Jun 2, 2016
 * @author kyuksel
 *         date: Jul 15, 2016
 */
final class RxExecuterTest extends ExecuterTest {
  override def makeExecuter: LExecuter = RxExecuter.default

  import RxExecuterTest._

  private val executer = RxExecuter.default

  private def addNoOp(executable: LExecutable): LExecutable = LExecutable(Set(RxMockJob("NoOp", executable.jobs)))

  private def executionCount(job: LJob): Int = job.asInstanceOf[RxMockJob].executionCount

  def flattenTree(tree: Set[LJob]): Set[LJob] = {
    tree.foldLeft(tree)((acc, x) =>
      x.inputs ++ flattenTree(x.inputs) ++ acc)
  }

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
    val job11 = RxMockJob("1st_step_job_1", delay = 1000) // scalastyle:ignore magic.number
    val job12 = RxMockJob("1st_step_job_2")
    val job21 = RxMockJob("2nd_step_job_1", Set(job11))
    val job22 = RxMockJob("2nd_step_job_2", Set(job11))
    val job23 = RxMockJob("2nd_step_job_3", Set(job12))
    val job24 = RxMockJob("2nd_step_job_4", Set(job12))
    val job31 = RxMockJob("3rd_step_job_1", Set(job21, job22))
    val job32 = RxMockJob("3rd_step_job_2", Set(job23, job24))
    val job4 = RxMockJob("4th_step_job", Set(job31, job32))

    val executable = LExecutable(Set(job4))

    val flattenedJobs = flattenTree(Set(job4))

    val maxSimultaneousJobs = 5
    val mockRunner = MockChunkRunner(AsyncLocalChunkRunner, maxSimultaneousJobs)
    val executer = RxExecuter(mockRunner)

    executer.execute(executable)
    val chunks = mockRunner.chunks
    val expectedMaxSimultaneousJobs = 2
    assert(mockRunner.chunks.forall(_.size <= expectedMaxSimultaneousJobs))
  }
}

object RxExecuterTest {

  private final case class RxMockJob(name: String, inputs: Set[LJob] = Set.empty, delay: Int = 0) extends LJob {
    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    private[this] val lock = new AnyRef

    private[this] var _executionCount = 0

    def executionCount = lock.synchronized(_executionCount)


    override def execute(implicit context: ExecutionContext): Future[Result] = {
      lock.synchronized(_executionCount += 1)
      Thread.sleep(delay)
      isSuccessful() = true
      Future.successful(LJob.SimpleSuccess(name))
    }
  }

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
