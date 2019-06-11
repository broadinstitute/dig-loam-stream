package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.LJob
import loamstream.model.jobs.Execution

/**
 * @author clint
 * May 28, 2019
 */
final class ExecutionRecorderTest extends FunSuite {
  test("&&") {
    var flag0 = 0
    var flag1 = 10
    
    def mockExecutionRecorder[A](body: => A): ExecutionRecorder = new ExecutionRecorder {
      override def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit = {
        body
        ()
      }
    }
    
    val er0 = mockExecutionRecorder { flag0 += 1}
    val er1 = mockExecutionRecorder { flag1 += 1}
    
    val composite = er0 && er1
    
    assert(flag0 === 0)
    assert(flag1 === 10)
    
    composite.record(TestHelpers.DummyJobOracle, Nil)
    
    assert(flag0 === 1)
    assert(flag1 === 11)
    
    composite.record(TestHelpers.DummyJobOracle, Nil)
    
    assert(flag0 === 2)
    assert(flag1 === 12)
  }
}
