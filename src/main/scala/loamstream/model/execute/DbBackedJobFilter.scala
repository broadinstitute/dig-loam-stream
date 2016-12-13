package loamstream.model.execute

import java.nio.file.Path

import loamstream.db.LoamDao
import loamstream.model.jobs.{Execution, LJob, Output, OutputRecord}
import loamstream.model.jobs.Output.PathOutput
import loamstream.util.Loggable
import loamstream.util.TimeEnrichments

/**
 * @author clint
 * date: Sep 30, 2016
 */
final class DbBackedJobFilter(val dao: LoamDao) extends JobFilter with Loggable {
  override def shouldRun(dep: LJob): Boolean = {
    def needsToBeRun(output: Output): Boolean = output match {
      case Output.PathBased(p) => {
        val path = normalize(p)
        output.isMissing || isOlder(path) || notHashed(path) || hasDifferentHash(path)
      }
      case _ => true
    }

    dep.outputs.isEmpty || dep.outputs.exists(needsToBeRun)
  }

  override def record(executions: Iterable[Execution]): Unit = {
    //NB: We can only insert command executions (UGER or command-line jobs, anything with an in exit status code) 
    //for now
    val insertableExecutions = executions.collect { case e if e.isCommandExecution => hashOutputsOf(e) }

    debug(s"RECORDING $insertableExecutions")
    
    dao.insertExecutions(insertableExecutions)
  }

  private def hashOutputsOf(e: Execution): Execution = {
    e.transformOutputs { outputs =>
      outputs.collect { case Output.PathBased(path) =>
        val normalized = normalize(path)
        
        if(e.isSuccess) cachedOutput(normalized) else Output.PathOutput(normalized)
      }
    }
  }

  private def normalize(p: Path) = p.toAbsolutePath

  private def findOutput(loc: String): Option[OutputRecord] = {
    dao.findOutputRecord(loc)
  }
  
  private def isHashed(loc: String): Boolean = {
    findOutput(loc).isDefined
  }

  private def notHashed(output: Path): Boolean = !isHashed(output)

  private def hasDifferentHash(output: Path): Boolean = {
    //TODO: Other hash types
    def hash(p: Path) = PathOutput(p).hash

    val path = normalize(output)

    findOutput(path) match {
      case Some(cachedOutput) => cachedOutput.hash != hash(path)
      case None               => true
    }
  }

  private def isOlder(output: Path): Boolean = {
    import TimeEnrichments.Implicits._

    def lastModified(p: Path) = PathOutput(p).lastModified

    val path = normalize(output)

    findOutput(path) match {
      case Some(cachedOutput) => lastModified(path) < cachedOutput.lastModified
      case None               => false
    }
  }
}