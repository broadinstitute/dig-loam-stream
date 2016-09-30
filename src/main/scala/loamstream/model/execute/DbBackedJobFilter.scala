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
    val (insertedSuccesses, _) = insertExecutions(executions)
    
    updateCache(insertedSuccesses)
  }
  
  private def updateCache(successes: Iterable[Execution]): Unit = {
    // :(
    val allOutputs = successes.flatMap(_.outputs).collect {
      case cached: CachedOutput => cached
    }

    import Traversables.Implicits._
    
    val newOutputMap = allOutputs.mapBy(_.path)

    cachedOutputsByPath.mutate { oldOutputs =>
      oldOutputs ++ newOutputMap
    }
  }
  
  private def insertExecutions(executions: Iterable[Execution]): (Iterable[Execution], Iterable[Execution]) = {
    //NB: We can only insert command executions (UGER or command-line jobs, anything with an in exit status code) 
    //for now
    val insertableExecutions = executions.collect { case e if e.isCommandExecution => e }

    //Insert successes and all their outputs
    val insertableSuccesses = insertableExecutions.collect {
      case e @ Execution(cr: CommandResult, _) if cr.isSuccess => hashOutputsOf(e)
    }

    //Don't hash or store outputs of failures 
    val insertableFailures = insertableExecutions.collect {
      case e @ Execution(cr: CommandResult, _) if cr.isFailure => e.withOutputs(Set.empty)
    }

    dao.insertExecutions(insertableSuccesses ++ insertableFailures)
    
    (insertableSuccesses, insertableFailures)
  }

  private def cachedOutput(path: Path): CachedOutput = PathOutput(path).toCachedOutput

  private def hashOutputsOf(e: Execution): Execution = {
    e.transformOutputs { outputs =>
      outputs.collect { case Output.PathBased(path) => cachedOutput(normalize(path)) }
    }
  }

  //TODO: Support outputs other than Paths
  //TODO: Use this at all?
  private[this] lazy val cachedOutputsByPath: ValueBox[Map[Path, CachedOutput]] = {
    //TODO: All of them?  
    val map: Map[Path, CachedOutput] = dao.allOutputs.map(row => row.path -> row).toMap

    if (isDebugEnabled) {
      debug(s"Known paths: ${map.size}")

      map.values.foreach { data =>
        debug(data.toString)
      }
    }

    ValueBox(map)
  }
  
  private[execute] def cache: Map[Path, CachedOutput] = cachedOutputsByPath.value

  private def normalize(p: Path) = p.toAbsolutePath

  private def isHashed(output: Path): Boolean = {
    cachedOutputsByPath.value.contains(normalize(output))
  }

  private def notHashed(output: Path): Boolean = !isHashed(output)

  private def hasDifferentHash(output: Path): Boolean = {
    //TODO: Other hash types
    def hash(p: Path) = PathOutput(p).hash

    val path = normalize(output)

    cachedOutputsByPath.value.get(path) match {
      case Some(cachedOutput) => cachedOutput.hash != hash(path)
      case None               => true
    }
  }

  private def isOlder(output: Path): Boolean = {
    import TimeEnrichments.Implicits._

    def lastModified(p: Path) = PathOutput(p).lastModified

    val path = normalize(output)

    cachedOutputsByPath.value.get(path) match {
      case Some(cachedOutput) => lastModified(path) < cachedOutput.lastModified
      case None               => false
    }
  }
}