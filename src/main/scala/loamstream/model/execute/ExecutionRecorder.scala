package loamstream.model.execute

import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob

/**
 * @author clint
 * Jul 2, 2018
 */
trait ExecutionRecorder {
  def record(executionTuples: Iterable[(LJob, Execution)]): Unit
}

object ExecutionRecorder {
  object DontRecord extends ExecutionRecorder {
    override def record(executionTuples: Iterable[(LJob, Execution)]): Unit = ()
  }
}
