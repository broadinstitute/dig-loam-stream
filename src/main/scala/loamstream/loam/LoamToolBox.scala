package loamstream.loam

import java.nio.file.{ Path, Paths }

import loamstream.LEnv
import loamstream.loam.files.LoamFileManager
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.jobs.{ LJob, LToolBox }
import loamstream.model.{ AST, LPipeline, Tool }
import loamstream.util.{ Hit, Miss, Shot, Shots, Snag }

/**
 * LoamStream
 * Created by oliverr on 6/21/2016.
 */
final case class LoamToolBox(env: LEnv) extends LToolBox {

  private val fileManager = new LoamFileManager

  @volatile private[this] var loamJobs: Map[LoamTool, LJob] = Map.empty

  private[this] val lock = new AnyRef

  private[loam] def newLoamJob(tool: LoamTool): Shot[LJob] = {
    tool.graphBuilder.applyEnv(env)

    val graph = tool.graphBuilder.graph

    val commandLineString = graph.toolTokens(tool).map(_.toString(env, fileManager)).mkString

    val workDir: Path = graph.workDirOpt(tool).getOrElse(Paths.get("."))

    val shotsForPrecedingTools: Shot[Set[LJob]] = Shot.sequence(graph.toolsPreceding(tool).map(getLoamJob))
    
    shotsForPrecedingTools.map { inputJobs =>
      CommandLineStringJob(commandLineString, workDir, inputJobs)
    }
  }

  private[loam] def getLoamJob(tool: LoamTool): Shot[LJob] = lock.synchronized {
    loamJobs.get(tool) match {
      case Some(job) => Hit(job)
      case _ => newLoamJob(tool) match {
        case jobHit @ Hit(job) =>
          loamJobs += tool -> job
          jobHit
        case miss: Miss => miss
      }
    }
  }

  override def toolToJobShot(tool: Tool): Shot[LJob] = tool match {
    case loamTool: LoamTool => getLoamJob(loamTool)
    case _                  => Miss(Snag(s"LoamToolBox only knows Loam tools; it doesn't know about $tool."))
  }

}
