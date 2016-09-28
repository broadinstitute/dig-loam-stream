package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.model.jobs.Output
import loamstream.db.LoamDao
import loamstream.util.Loggable
import java.nio.file.Path
import loamstream.util.ValueBox
import loamstream.model.jobs.Output.CachedOutput
import loamstream.model.jobs.Output.PathOutput
import loamstream.util.TimeEnrichments
import loamstream.util.Traversables

/**
 * @author clint
 * date: Aug 26, 2016
 */
trait JobFilter {
  def shouldRun(job: LJob): Boolean

  def record(outputs: Iterable[Output]): Unit
}

object JobFilter {
  object RunEverything extends JobFilter {
    override def shouldRun(job: LJob): Boolean = true

    override def record(outputs: Iterable[Output]): Unit = ()
  }

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

    override def record(outputs: Iterable[Output]): Unit = {
      val outputPaths = outputs.collect { case Output.PathBased(path) => normalize(path) }
      
      def cachedOutput(path: Path): CachedOutput = PathOutput(path).toCachedOutput
      
      import Traversables.Implicits._
      
      val newOutputs = outputPaths.mapTo(cachedOutput)
      
      cachedOutputsByPath.mutate { oldOutputs =>
        oldOutputs ++ newOutputs 
      }
      
      dao.insertOrUpdate(newOutputs.values)
    }

    //Support outputs other than Paths
    private[this] lazy val cachedOutputsByPath: ValueBox[Map[Path, CachedOutput]] = {
      //TODO: All of them?  
      val map: Map[Path, CachedOutput] = dao.allRows.map(row => row.path -> row).toMap

      if (isDebugEnabled) {
        debug(s"Known paths: ${map.size}")

        map.values.foreach { data =>
          debug(data.toString)
        }
      }

      ValueBox(map)
    }
    
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
}