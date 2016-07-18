package loamstream.model.execute

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result

/**
  * Created by kyuksel on 7/15/16.
  */
final class ChunkedExecuterTest extends ExecuterTest {
  override def makeExecuter: LExecuter = ChunkedExecuter.default
  
  import ChunkedExecuterTest._
  
  private val executer = ChunkedExecuter.default

  private def addNoOp(executable: LExecutable): LExecutable = LExecutable(Set(MockJob("NoOp", executable.jobs)))
  
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
    val firstStepJob = MockJob("1st_step")
    val secondStepJob1 = MockJob("2nd_step_job_1", Set(firstStepJob))
    val secondStepJob2 = MockJob("2nd_step_job_2", Set(firstStepJob))
    val secondStepJob3 = MockJob("2nd_step_job_3", Set(firstStepJob))
    
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
    val firstStepJob = MockJob("1st_step")
    val secondStepJob1 = MockJob("2nd_step_job_1", Set(firstStepJob))
    val secondStepJob2 = MockJob("2nd_step_job_2", Set(firstStepJob))
    val secondStepJob3 = MockJob("2nd_step_job_3", Set(firstStepJob))
    val thirdStepJob = MockJob("3rd_step", Set(secondStepJob1, secondStepJob2, secondStepJob3))
    
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
}

object ChunkedExecuterTest {
  private final case class MockJob(name: String, inputs: Set[LJob] = Set.empty) extends LJob {
    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
    
    private[this] val lock = new AnyRef
    
    private[this] var _executionCount = 0
    
    def executionCount = lock.synchronized(_executionCount)
    
    override def execute(implicit context: ExecutionContext): Future[Result] = {
      lock.synchronized(_executionCount += 1)
      
      Future.successful(LJob.SimpleSuccess(name))
    }
  }
}