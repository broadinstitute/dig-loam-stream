package loamstream.model.execute

import loamstream.model.jobs.Execution

/**
 * @author clint
 * Jul 2, 2018
 */
trait ExecutionRecorder {
  def record(executions: Iterable[Execution]): Unit
}

object ExecutionRecorder {
  object DontRecord extends ExecutionRecorder {
    override def record(executions: Iterable[Execution]): Unit = ()
  }
}
