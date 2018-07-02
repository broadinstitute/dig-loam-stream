package loamstream.model.execute

import loamstream.util.Loggable
import loamstream.db.LoamDao
import loamstream.model.jobs.Execution

/**
 * @author clint
 * Jul 2, 2018
 */
final class DbBackedExecutionRecorder(val dao: LoamDao) extends ExecutionRecorder with Loggable {
  
  override def record(executions: Iterable[Execution]): Unit = {
    //NB: We can only insert command executions (UGER or command-line jobs, anything with an in exit status code)
    //for now
    val insertableExecutions = executions.filter(_.isCommandExecution)

    dao.insertExecutions(insertableExecutions)
  }
}
