package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.util.ValueBox
import loamstream.model.jobs.Execution

/**
 * @author clint
 * Mar 21, 2017
 */
final class MockJobFilterAndExecutionRecorder(
    shouldRunPredicate: LJob => Boolean = _ => true) extends JobFilter with ExecutionRecorder {

  val recordedExecutionTuples: ValueBox[Seq[(LJob, Execution)]] = ValueBox(Vector.empty)
  
  override def shouldRun(job: LJob): Boolean = shouldRunPredicate(job)

  override def record(executionTuples: Iterable[(LJob, Execution)]): Unit = {
    recordedExecutionTuples.mutate(_ ++ executionTuples)
  }
}
