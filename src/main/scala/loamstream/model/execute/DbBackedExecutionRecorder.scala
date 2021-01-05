package loamstream.model.execute

import loamstream.util.Loggable
import loamstream.db.LoamDao
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.StoreRecord

/**
 * @author clint
 * Jul 2, 2018
 */
final class DbBackedExecutionRecorder(
    val dao: LoamDao, 
    hashingStrategy: HashingStrategy) extends ExecutionRecorder with Loggable {
  
  override def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit = {
    //NB: We can only insert skipped executions and command executions (UGER or command-line jobs, 
    //anything with an in exit status code) for now
    
    val insertableExecutions = executionTuples.collect { case (_, e) if e.isPersistable => e }
    
    import DbBackedExecutionRecorder.noHashes
    
    debug(s"Recording job executions ${if(hashingStrategy.shouldHash) "WITH" else "WITHOUT"} hashes")
    
    val disableHashingIfNeeded: Execution => Execution = hashingStrategy match {
      case HashingStrategy.DontHashOutputs => noHashes
      case HashingStrategy.HashOutputs => identity
    }

    dao.insertExecutions(insertableExecutions.map(disableHashingIfNeeded))
  }
}

object DbBackedExecutionRecorder {
  private val noHash: () => Option[String] = () => None
  
  private def noHashes(sr: StoreRecord): StoreRecord = sr.copy(makeHash = noHash, makeHashType = noHash)
  
  private def noHashes(e: Execution): Execution = e.copy(outputs = e.outputs.map(noHashes))
}
