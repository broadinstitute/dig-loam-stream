package loamstream.model.execute

import java.nio.file.Path

import loamstream.db.LoamDao
import loamstream.model.jobs.{Execution, LJob, OutputRecord}
import loamstream.util.Loggable

/**
 * @author clint
 *         kyuksel
 * date: Sep 30, 2016
 */
final class DbBackedJobFilter(val dao: LoamDao) extends JobFilter with Loggable {
  override def shouldRun(dep: LJob): Boolean = {
    val noOutputs = dep.outputs.isEmpty
    
    def anyOutputNeedsToBeRun = dep.outputs.exists(o => needsToBeRun(o.toOutputRecord))
    
    noOutputs || anyOutputNeedsToBeRun
  }

  override def record(executions: Iterable[Execution]): Unit = {
    //NB: We can only insert command executions (UGER or command-line jobs, anything with an in exit status code) 
    //for now
    val insertableExecutions = executions.filter(_.isCommandExecution)

    debug(s"RECORDING $insertableExecutions")
    
    dao.insertExecutions(insertableExecutions)
  }

  private[execute] def needsToBeRun(rec: OutputRecord): Boolean = {
    rec.isMissing || isOlder(rec) || notHashed(rec) || hasDifferentHash(rec)
  }

  private def normalize(p: Path) = p.toAbsolutePath

  private def findOutput(loc: String): Option[OutputRecord] = {
    dao.findOutputRecord(loc)
  }
  
  private def isHashed(rec: OutputRecord): Boolean = {
    findOutput(rec.loc) match {
      case Some(matchingRec) => matchingRec.isHashed
      case None => false
    }
  }

  private def notHashed(rec: OutputRecord): Boolean = !isHashed(rec)

  private[execute] def hasDifferentHash(rec: OutputRecord): Boolean = {
    findOutput(rec.loc) match {
      case Some(matchingRec) => matchingRec.hasDifferentHashThan(rec)
      case None => false
    }
  }

  private[execute] def isOlder(currentRec: OutputRecord): Boolean = {
    findOutput(currentRec.loc) match {
      case Some(matchingRec) => currentRec.isOlderThan(matchingRec)
      case None => false
    }
  }
}
