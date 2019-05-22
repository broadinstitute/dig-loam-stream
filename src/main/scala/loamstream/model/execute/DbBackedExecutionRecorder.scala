package loamstream.model.execute

import loamstream.util.Loggable
import loamstream.db.LoamDao
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob

/**
 * @author clint
 * Jul 2, 2018
 */
final class DbBackedExecutionRecorder(val dao: LoamDao) extends ExecutionRecorder {
  
  override def record(executionTuples: Iterable[(LJob, Execution)]): Unit = {
    //NB: We can only insert command executions (UGER or command-line jobs, anything with an in exit status code)
    //for now
    val insertableExecutions = executionTuples.collect { case (_, e) if e.isCommandExecution => e }

    dao.insertExecutions(insertableExecutions)
  }
}
