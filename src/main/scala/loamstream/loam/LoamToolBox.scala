package loamstream.loam

import java.net.URI
import java.nio.file.{Path, Paths}

import loamstream.googlecloud.CloudStorageClient
import loamstream.model.{Store, Tool}
import loamstream.model.execute.{Environment, Executable}
import loamstream.model.jobs.{JobNode, Output}
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.drm.DockerParams
import loamstream.model.execute.Locations

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
        case jobOpt @ Some(job) =>
          loamJobs += tool -> job
          jobOpt
        case None => None
      }
    }
  }

  private[loam] def newJob(graph: LoamGraph)(tool: Tool): Option[JobNode] = {
    val workDir: Path = graph.workDirOpt(tool).getOrElse(Paths.get("."))

    val environment: Environment = graph.executionEnvironmentOpt(tool).getOrElse(Environment.Local)

    val dockerParamsOpt: Option[DockerParams] = environment match {
      case Environment.Lsf(lsfSettings) => lsfSettings.dockerParams 
      case _ => None
    }
    
    val inputJobs = toJobs(graph)(graph.toolsPreceding(tool))

    val outputs = outputsFor(graph, tool, dockerParamsOpt)

    val toolNameOpt = graph.nameOf(tool)

    tool match {
      case cmdTool: LoamCmdTool =>
        Some(CommandLineJob(cmdTool.commandLine, workDir, environment, inputJobs, outputs, nameOpt = toolNameOpt))
      case _ => None
    }
  }

  private def outputsFor(graph: LoamGraph, tool: Tool, dockerParamsOpt: Option[DockerParams]): Set[Output] = {
    val loamStores: Set[Store] = graph.toolOutputs(tool)

    val locations: Locations[Path] = dockerParamsOpt.getOrElse(Locations.identity)
    
    def pathOrUriToOutput(store: Store): Option[Output] = {
      store.pathOpt.orElse(store.uriOpt).map {
        case path: Path => Output.PathOutput(locations.inHost(path), locations)
        case uri: URI   => Output.GcsUriOutput(uri, client)
      }
    }
    
    loamStores.flatMap(pathOrUriToOutput)
  }
}
