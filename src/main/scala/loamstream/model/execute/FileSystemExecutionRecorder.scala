package loamstream.model.execute

import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob

/**
 * @author clint
 * May 22, 2019
 */
final class FileSystemExecutionRecorder extends ExecutionRecorder {
  private[this] lazy val init: Unit = {
    
  }
  
  override def record(executionTuples: Iterable[(LJob, Execution)]): Unit = {
    
  }
}