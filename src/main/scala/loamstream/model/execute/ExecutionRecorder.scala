package loamstream.model.execute

import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobOracle

/**
 * @author clint
 * Jul 2, 2018
 */
trait ExecutionRecorder {
  def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit
  
  final def &&(rhs: ExecutionRecorder): ExecutionRecorder = ExecutionRecorder.CompositeExecutionRecorder(this, rhs)
}

object ExecutionRecorder {
  final case class CompositeExecutionRecorder(a: ExecutionRecorder, b: ExecutionRecorder) extends ExecutionRecorder {
    override def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit = {
      a.record(jobOracle, executionTuples)
      
      b.record(jobOracle, executionTuples)
    }
  }
  
  object DontRecord extends ExecutionRecorder {
    override def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit = ()
  }
}
