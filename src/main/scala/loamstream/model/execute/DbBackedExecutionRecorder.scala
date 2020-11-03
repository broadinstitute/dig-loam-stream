package loamstream.model.execute

import loamstream.util.Loggable
import loamstream.db.LoamDao
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobOracle

/**
 * @author clint
 * Jul 2, 2018
 */
final class DbBackedExecutionRecorder(val dao: LoamDao) extends ExecutionRecorder {
  
  override def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit = {
    //NB: We can only insert skipped executions and command executions (UGER or command-line jobs, 
    //anything with an in exit status code) for now
    
    val insertableExecutions = executionTuples.collect { case (_, e) if e.isPersistable => e }

    dao.insertExecutions(insertableExecutions)
  }
}
