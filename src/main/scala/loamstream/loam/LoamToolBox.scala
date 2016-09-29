package loamstream.loam

import java.nio.file.{Path, Paths}

import loamstream.model.Tool
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.jobs.{LJob, LToolBox, NativeJob}
import loamstream.util.{Hit, Miss, Shot, Snag}
import loamstream.loam.LoamGraph.StoreEdge.PathEdge
import loamstream.loam.LoamGraph.StoreEdge.ToolEdge
import loamstream.model.jobs.Output

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
final class LoamToolBox extends LToolBox {

  @volatile private[this] var loamJobs: Map[LoamTool, LJob] = Map.empty

  private[this] val lock = new AnyRef

  private[loam] def newLoamJob(tool: LoamTool): Shot[LJob] = {
    val graph = tool.graphBox.value

    def pathOutputsFor(tool: LoamTool): Set[Output] = {
      val loamStores: Set[LoamStore] = graph.toolOutputs(tool)

      loamStores.flatMap(_.pathOpt).map(Output.PathOutput)
    }

    val workDir: Path = graph.workDirOpt(tool).getOrElse(Paths.get("."))

    val shotsForPrecedingTools: Shot[Set[LJob]] = Shot.sequence(graph.toolsPreceding(tool).map(getLoamJob))

    shotsForPrecedingTools.map { inputJobs =>
      val outputs = pathOutputsFor(tool)

      tool match {
        case cmdTool: LoamCmdTool =>
          val commandLineString = cmdTool.tokens.map(_.toString(cmdTool.context.fileManager)).mkString
          CommandLineStringJob(commandLineString, workDir, inputJobs, outputs)
        case nativeTool: LoamNativeTool[_] => NativeJob(nativeTool.wrappedExpBox, inputJobs, outputs)
      }
    }
  }

  private[loam] def getLoamJob(tool: LoamTool): Shot[LJob] = lock.synchronized {
    loamJobs.get(tool) match {
      case Some(job) => Hit(job)
      case _ => newLoamJob(tool) match {
        case jobHit@Hit(job) =>
          loamJobs += tool -> job
          jobHit
        case miss: Miss => miss
      }
    }
  }

  override def toolToJobShot(tool: Tool): Shot[LJob] = tool match {
    case loamTool: LoamTool => getLoamJob(loamTool)
    case _ => Miss(Snag(s"LoamToolBox only knows Loam tools; it doesn't know about $tool."))
  }
}
