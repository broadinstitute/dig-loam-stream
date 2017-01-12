package loamstream.loam

import java.net.URI
import java.nio.file.{Path, Paths}

import loamstream.loam.ops.filters.LoamStoreFilterTool
import loamstream.loam.ops.mappers.LoamStoreMapperTool
import loamstream.model.Tool
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.jobs.ops.{StoreFilterJob, StoreMapperJob}
import loamstream.model.jobs.{LJob, LToolBox, NativeJob, Output}
import loamstream.util.{Hit, Miss, Shot, Snag}
import loamstream.model.execute.ExecutionEnvironment

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
final class LoamToolBox(context: LoamProjectContext) extends LToolBox {

  @volatile private[this] var loamJobs: Map[LoamTool, LJob] = Map.empty

  private[this] val lock = new AnyRef

  private[loam] def newLoamJob(tool: LoamTool): Shot[LJob] = {
    val graph = tool.graphBox.value

    def outputsFor(tool: LoamTool): Set[Output] = {
      val loamStores: Set[LoamStore.Untyped] = graph.toolOutputs(tool)

      def pathOrUriToOutput(store: LoamStore.Untyped): Option[Output] = {
        store.pathOpt.orElse(store.uriOpt).map {
          case path: Path => Output.PathOutput(path)
          case uri: URI => Output.GcsUriOutput(uri)
        }
      }

      loamStores.flatMap(pathOrUriToOutput(_))
    }

    val workDir: Path = graph.workDirOpt(tool).getOrElse(Paths.get("."))
    
    val environment: ExecutionEnvironment = graph.executionEnvironmentOpt(tool).getOrElse(ExecutionEnvironment.Local)

    val shotsForPrecedingTools: Shot[Set[LJob]] = Shot.sequence(graph.toolsPreceding(tool).map(getLoamJob))

    shotsForPrecedingTools.map { inputJobs =>
      val outputs = outputsFor(tool)

      tool match {
        case cmdTool: LoamCmdTool =>
          CommandLineStringJob(cmdTool.commandLine, workDir, environment, inputJobs, outputs)
        case storeFilterTool: LoamStoreFilterTool[_] =>
          val inStore = graph.toolInputs(tool).head
          val outStore = graph.toolOutputs(tool).head
          StoreFilterJob(inStore.path, outStore.path, inStore.sig.tpe, inputJobs, outputs, storeFilterTool.filter)
        case storeMapperTool: LoamStoreMapperTool[_, _] =>
          val inStore = graph.toolInputs(tool).head
          val outStore = graph.toolOutputs(tool).head
          StoreMapperJob(inStore.path, outStore.path, inStore.sig.tpe, outStore.sig.tpe, inputJobs, outputs,
            storeMapperTool.mapper)
        case nativeTool: LoamNativeTool[_] => NativeJob(nativeTool.expBox, inputJobs, outputs)
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
