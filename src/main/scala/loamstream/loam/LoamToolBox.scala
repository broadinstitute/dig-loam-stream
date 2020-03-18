package loamstream.loam

import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths

import loamstream.googlecloud.CloudStorageClient
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.execute.Executable
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.Settings
import loamstream.model.jobs.NativeJob

/**
 * LoamStream
 * 
 * Created by oliverr on 6/21/2016.
 * 
 * Turns a LoamGraph into an Executable (a collection of jobs)
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

    val settings: Settings = graph.settingsOpt(tool).getOrElse(LocalSettings)

    val dependencyJobs = toJobs(graph)(graph.toolsPreceding(tool))
    def successorJobs = toJobs(graph)(graph.toolsSucceeding(tool))

    val inputs = inputsFor(graph, tool)
    val outputs = outputsFor(graph, tool)

    val toolNameOpt = graph.nameOf(tool)

    tool match {
      case cmdTool: LoamCmdTool => {
        Some(CommandLineJob(
            commandLineString = cmdTool.commandLine, 
            workDir = workDir, 
            initialSettings = settings, 
            dependencies = dependencyJobs,
            successorsFn = () => successorJobs, 
            inputs = inputs,
            outputs = outputs, 
            nameOpt = toolNameOpt))
      }
      case nativeTool: NativeTool => {
        Some(NativeJob(
            body = nativeTool.body,
            initialSettings = settings, 
            dependencies = inputJobs,
            inputs = inputs,
            outputs = outputs, 
            nameOpt = toolNameOpt))
      }
      case _ => None
    }
  }

  private def inputsFor(graph: LoamGraph, tool: Tool): Set[DataHandle] = handlesFor(graph.toolInputs(tool))
  
  private def outputsFor(graph: LoamGraph, tool: Tool): Set[DataHandle] = handlesFor(graph.toolOutputs(tool))
  
  private def handlesFor(stores: Set[Store]): Set[DataHandle] = {
    def pathOrUriToOutput(store: Store): Option[DataHandle] = {
      store.pathOpt.orElse(store.uriOpt).map {
        case path: Path => DataHandle.PathHandle(path)
        case uri: URI   => DataHandle.GcsUriHandle(uri, client)
      }
    }
    
    stores.flatMap(pathOrUriToOutput)
  }
}
