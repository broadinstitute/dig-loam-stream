package loamstream.model.execute

import com.typesafe.config.ConfigFactory
import loamstream.conf.UgerConfig
import loamstream.model.execute.ChunkedExecuter.AsyncLocalChunkRunner

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.Output
import loamstream.util.ValueBox
import loamstream.model.jobs.MockJob

/**
  * @author clint
  * date: Jun 2, 2016
  * 
  * @author kyuksel 
  * date: Jul 15, 2016
  */
final class ChunkedExecuterTest extends ExecuterTest {
  override def makeExecuter: LExecuter = ChunkedExecuter.default
  
  import ChunkedExecuterTest._
  
  private val executer = ChunkedExecuter.default

  private def success(name: String) = LJob.SimpleSuccess(name)
  
  private def addNoOp(executable: LExecutable) = LExecutable(Set(MockJob(success("NoOp"), "NoOp", executable.jobs)))
  
  private def executionCount(job: LJob): Int = job.asInstanceOf[MockJob].executionCount
  
  test("Parallel pipeline mocking imputation results in expected number of steps") {
    /* Two-step pipeline to result in 4 executions:
     *
     *            Impute0
     *           /
     * ShapeIt -- Impute1
     *           \
     *            Impute2
     */
    val firstStepJob = MockJob(success("1st_step"))
    val secondStepJob1 = MockJob(success("2nd_step_job_1"), inputs = Set(firstStepJob))
    val secondStepJob2 = MockJob(success("2nd_step_job_2"), inputs = Set(firstStepJob))
    val secondStepJob3 = MockJob(success("2nd_step_job_3"), inputs = Set(firstStepJob))
    
    val twoStepExecutable = addNoOp(LExecutable(Set(secondStepJob1, secondStepJob2, secondStepJob3)))
    
    assert(firstStepJob.executionCount == 0)
    
    assert(secondStepJob1.executionCount == 0)
    assert(secondStepJob2.executionCount == 0)
    assert(secondStepJob3.executionCount == 0)
    
    assert(executionCount(twoStepExecutable.jobs.head) == 0)
    
    val results = executer.execute(twoStepExecutable)
    
    def withName(n: String): MockJob = results.keySet.map(_.asInstanceOf[MockJob]).find(_.name == n).get
    
    val step1 = withName(firstStepJob.name)
    val step21 = withName(secondStepJob1.name)
    val step22 = withName(secondStepJob2.name)
    val step23 = withName(secondStepJob3.name)
    val step3 = withName("NoOp")
    
    assert(executionCount(step1) == 1)
    
    assert(executionCount(step21) == 1)
    assert(executionCount(step22) == 1)
    assert(executionCount(step23) == 1)
    
    assert(executionCount(step3) == 1)
  }
  
  test("Parallel pipeline mocking imputation (with QC) results in expected number of steps") {
    /* Three-step pipeline to result in 5 more executions:
     *
     *            Impute0
     *           /        \
     * ShapeIt -- Impute1 -- QC
     *           \        /
     *            Impute2
     */
    val firstStepJob = MockJob(success("1st_step"))
    val secondStepJob1 = MockJob(success("2nd_step_job_1"), inputs = Set(firstStepJob))
    val secondStepJob2 = MockJob(success("2nd_step_job_2"), inputs = Set(firstStepJob))
    val secondStepJob3 = MockJob(success("2nd_step_job_3"), inputs = Set(firstStepJob))
    val thirdStepJob = MockJob(success("3rd_step"), inputs = Set(secondStepJob1, secondStepJob2, secondStepJob3))
    
    val threeStepExecutable = LExecutable(Set(thirdStepJob))
    
    assert(firstStepJob.executionCount == 0)
    
    assert(secondStepJob1.executionCount == 0)
    assert(secondStepJob2.executionCount == 0)
    assert(secondStepJob3.executionCount == 0)
    
    assert(thirdStepJob.executionCount == 0)
    
    val results = executer.execute(threeStepExecutable)
    
    def withName(n: String): MockJob = results.keySet.map(_.asInstanceOf[MockJob]).find(_.name == n).get
    
    val step1 = withName(firstStepJob.name)
    val step21 = withName(secondStepJob1.name)
    val step22 = withName(secondStepJob2.name)
    val step23 = withName(secondStepJob3.name)
    val step3 = withName(thirdStepJob.name)
    
    assert(executionCount(step1) == 1)
    
    assert(executionCount(step21) == 1)
    assert(executionCount(step22) == 1)
    assert(executionCount(step23) == 1)
    
    assert(executionCount(step3) == 1)
  }

  test("Number of jobs run at a time doesn't exceed specified limit") {
    /* Three-step pipeline to result in 5 more executions:
     *
     *            Impute0
     *           /        \
     * ShapeIt -- Impute1 -- QC
     *           \        /
     *            Impute2
     */
    val firstStepJob = MockJob(success("1st_step"))
    val secondStepJob1 = MockJob(success("2nd_step_job_1"), inputs = Set(firstStepJob))
    val secondStepJob2 = MockJob(success("2nd_step_job_2"), inputs = Set(firstStepJob))
    val secondStepJob3 = MockJob(success("2nd_step_job_3"), inputs = Set(firstStepJob))
    val thirdStepJob = MockJob(success("3rd_step"), inputs = Set(secondStepJob1, secondStepJob2, secondStepJob3))

    val threeStepExecutable = LExecutable(Set(thirdStepJob))

    val maxNumJobs = 2
    val mockRunner = MockChunkRunner(AsyncLocalChunkRunner, maxNumJobs)
    val executer = ChunkedExecuter(mockRunner)
    executer.execute(threeStepExecutable)

    assert(mockRunner.chunks.forall(_.size <= maxNumJobs))
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
    val job11 = MockJob(success("1st_step_job_1"), delay = 1000) // scalastyle:ignore magic.number
    val job12 = MockJob(success("1st_step_job_2"))
    val job21 = MockJob(success("2nd_step_job_1"), inputs = Set(job11))
    val job22 = MockJob(success("2nd_step_job_2"), inputs = Set(job11))
    val job23 = MockJob(success("2nd_step_job_3"), inputs = Set(job12))
    val job24 = MockJob(success("2nd_step_job_4"), inputs = Set(job12))
    val job31 = MockJob(success("3rd_step_job_1"), inputs = Set(job21, job22))
    val job32 = MockJob(success("3rd_step_job_2"), inputs = Set(job23, job24))
    val job4  = MockJob(success("4th_step_job")  , inputs = Set(job31, job32))

    val executable = LExecutable(Set(job4))

    val maxSimultaneousJobs = 5
    val mockRunner = MockChunkRunner(AsyncLocalChunkRunner, maxSimultaneousJobs)
    val executer = ChunkedExecuter(mockRunner)

    executer.execute(executable)
    val chunks = mockRunner.chunks
    val expectedMaxSimultaneousJobs = 2
    assert(mockRunner.chunks.forall(_.size <= expectedMaxSimultaneousJobs))
  }
}

object ChunkedExecuterTest {
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
