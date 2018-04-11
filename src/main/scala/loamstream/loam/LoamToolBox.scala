package loamstream.loam

import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths

import loamstream.googlecloud.CloudStorageClient
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.execute.Environment
import loamstream.model.execute.Executable
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.NativeJob
import loamstream.model.jobs.Output
import loamstream.model.jobs.commandline.CommandLineJob

/**
 * LoamStream
 * Created by oliverr on 6/21/2016.
 */
final class LoamToolBox(client: Option[CloudStorageClient] = None) {

  @volatile private[this] var loamJobs: Map[Tool, JobNode] = Map.empty

  private[this] val lock = new AnyRef

  def createExecutable(graph: LoamGraph): Executable = Executable(toJobs(graph)(graph.finalTools))

  private[loam] def toJobs(graph: LoamGraph)(tools: Set[Tool]): Set[JobNode] = tools.flatMap(getJob(graph))

  def getJob(graph: LoamGraph)(tool: Tool): Option[JobNode] = lock.synchronized {
    if (loamJobs.contains(tool)) {
      loamJobs.get(tool)
    } else {
      newJob(graph)(tool) match {
        case jobOpt @ Some(job) => {
          loamJobs += tool -> job
          jobOpt
        }
        case None => None
      }
    }
  }

  private[loam] def newJob(graph: LoamGraph)(tool: Tool): Option[JobNode] = {
    val workDir: Path = graph.workDirOpt(tool).getOrElse(Paths.get("."))

    val environment: Environment = graph.executionEnvironmentOpt(tool).getOrElse(Environment.Local)

    val inputJobs = toJobs(graph)(graph.toolsPreceding(tool))

    val outputs = outputsFor(graph)(tool)

    val toolNameOpt = graph.nameOf(tool)

    val dockerLocationOpt = graph.dockerLocations.get(tool)

    tool match {
      case cmdTool: LoamCmdTool =>
        Some(
          CommandLineJob(cmdTool.commandLine, workDir, environment, inputJobs, outputs, nameOpt = toolNameOpt,
            dockerLocationOpt = dockerLocationOpt)
        )
      case nativeTool: LoamNativeTool[_] =>
        Some(NativeJob(nativeTool.expBox, inputJobs, outputs, nameOpt = toolNameOpt))
      case _ => None
    }
  }

  private def outputsFor(graph: LoamGraph)(tool: Tool): Set[Output] = {
    val loamStores: Set[Store] = graph.toolOutputs(tool)

    def pathOrUriToOutput(store: Store): Option[Output] = {
      store.pathOpt.orElse(store.uriOpt).map {
        case path: Path => Output.PathOutput(path)
        case uri: URI   => Output.GcsUriOutput(uri, client)
      }
    }

    loamStores.flatMap(pathOrUriToOutput)
  }
}
