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

  val recordedExecutions: ValueBox[Seq[Execution]] = ValueBox(Vector.empty)
  
  override def shouldRun(job: LJob): Boolean = shouldRunPredicate(job)

  override def record(executions: Iterable[Execution]): Unit = {
    recordedExecutions.mutate(_ ++ executions)
  }
}
