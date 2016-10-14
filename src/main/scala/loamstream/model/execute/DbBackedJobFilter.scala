package loamstream.model.execute

import java.nio.file.Path

import loamstream.db.LoamDao
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobState.CommandResult
import loamstream.model.jobs.LJob
import loamstream.model.jobs.Output
import loamstream.model.jobs.Output.CachedOutput
import loamstream.model.jobs.Output.PathOutput
import loamstream.util.Loggable
import loamstream.util.TimeEnrichments
import loamstream.util.Traversables
import loamstream.util.ValueBox

/**
 * @author clint
 * date: Sep 30, 2016
 */
final class DbBackedJobFilter(dao: LoamDao) extends JobFilter with Loggable {
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

    dao.insertExecutions(insertableExecutions)
  }

  private def cachedOutput(path: Path): CachedOutput = PathOutput(path).toCachedOutput

  private def hashOutputsOf(e: Execution): Execution = {
    e.transformOutputs { outputs =>
      outputs.collect { case Output.PathBased(path) => cachedOutput(normalize(path)) }
    }
  }

  private def normalize(p: Path) = p.toAbsolutePath

  private def findOutput(path: Path): Option[Output] = {
    dao.findOutput(normalize(path))
  }
  
  private def isHashed(path: Path): Boolean = {
    findOutput(path).isDefined
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