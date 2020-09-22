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
import loamstream.util.Loggable
import loamstream.util.jvm.JvmArgs
import loamstream.cli.Conf
import loamstream.util.DirOracle
import loamstream.util.jvm.SysPropNames
import loamstream.util.DirTree.DirNode
import loamstream.conf.ExecutionConfig
import loamstream.util.ValueBox

/**
 * LoamStream
 * 
 * Created by oliverr on 6/21/2016.
 * 
 * Turns a LoamGraph into an Executable (a collection of jobs)
 */
final class LoamToolBox(client: Option[CloudStorageClient] = None) extends Loggable {

  @volatile private[this] var loamJobs: Map[Tool, JobNode] = Map.empty

  private[this] val lock = new AnyRef

  def createExecutable(graph: LoamGraph, executionConfig: ExecutionConfig = ExecutionConfig.default): Executable = {
    val oracle = LoamToolBox.makeDirOracle(executionConfig, graph)
    
    Executable(toJobs(graph, oracle, graph.finalTools))
  }

  private[loam] def toJobs(graph: LoamGraph, oracle: DirOracle[Tool], finalTools: Set[Tool]): Set[JobNode] = {
    finalTools.flatMap(getJob(graph, oracle))
  }

  def getJob(graph: LoamGraph, oracle: DirOracle[Tool])(tool: Tool): Option[JobNode] = lock.synchronized {
    loamJobs.get(tool) match {
      case s @ Some(_) => s
      case None => {
        val newJobOpt = newJob(graph, oracle, tool)
        
        newJobOpt.foreach { job =>
          loamJobs += tool -> job
          
          job
        }
        
        newJobOpt
      }
    }
  }

  private[loam] def newJob(graph: LoamGraph, oracle: DirOracle[Tool], tool: Tool): Option[JobNode] = {
    val workDir: Path = graph.workDirOpt(tool).getOrElse(Paths.get("."))

    val settings: Settings = graph.settingsOpt(tool).getOrElse(LocalSettings)

    val dependencyJobs = toJobs(graph, oracle, graph.toolsPreceding(tool))
    def successorJobs = toJobs(graph, oracle, graph.toolsSucceeding(tool))

    val inputs = inputsFor(graph, tool)
    val outputs = outputsFor(graph, tool)

    val toolNameOpt = graph.nameOf(tool)

    def commandLineJob(commandLine: String, settings: Settings = settings) = CommandLineJob(
      commandLineString = commandLine, 
      workDir = workDir, 
      initialSettings = settings, 
      dependencies = dependencyJobs,
      successorsFn = () => successorJobs, 
      inputs = inputs,
      outputs = outputs, 
      nameOpt = toolNameOpt)
    
    tool match {
      case cmdTool: LoamCmdTool => Some(commandLineJob(cmdTool.commandLine))
      case invokesLs: InvokesLsTool => {
        val commandLine = {
          val jvmArgs = invokesLs.scriptContext.lsSettings.jvmArgs
          val cliConfig = invokesLs.scriptContext.lsSettings.cliConfig
          
          cliConfig match {
            case Some(conf) => {
              val workDirOpt = oracle.dirOptFor(invokesLs)
              
              require(workDirOpt.isDefined, s"Couldn't determine work dir for tool $invokesLs")
              
              val workDir = workDirOpt.get
              
              val sysprops = Map(
                  SysPropNames.loamstreamWorkDir -> workDir.toString,
                  SysPropNames.loamstreamExecutionLoamstreamDir -> workDir.toString)
              
              val newConf = {
                conf.
                  withBackend(invokesLs.scriptContext.config.drmSystem.get).
                  withIsWorker(true).
                  withWorkDir(workDir).
                  onlyRun(graph.nameOf(invokesLs).get) //TODO
              }
              
              invokesLs.preambles ++ jvmArgs.rerunCommandTokens(newConf, sysprops)
            }
            case None => sys.error(
                s"In order to run in --worker mode, LS must be run from the command line, but no CLI config was found")
          }
        }
        
        Some(commandLineJob(commandLine.mkString(" ")))
      }
      case nativeTool: NativeTool => {
        Some(NativeJob(
            body = nativeTool.body,
            initialSettings = LocalSettings, 
            dependencies = dependencyJobs,
            successorsFn = () => successorJobs, 
            inputs = inputs,
            outputs = outputs, 
            nameOpt = toolNameOpt))
      }
      case t => {
        warn(s"Not mapping tool with unknown type: $t")
        
        None
      }
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

object LoamToolBox {
  private[loam] final class ToolsCanBeASimplePaths(graph: LoamGraph) extends DirNode.CanBeASimplePath[Tool] {
    override def toSimplePathName(t: Tool): String = {
      val nameOpt = graph.nameOf(t)
      
      require(nameOpt.isDefined, s"Only tools with user-specified names can be run by worker instances")
          
      val name = nameOpt.get
      
      require(
          !LoamGraph.isAutogenerated(name), 
          s"Only tools with user-specified names can be run by worker instances but got '$name'")
      
      loamstream.util.Paths.mungePathRelatedChars(name)
    }
  }
  
  private[loam] def makeDirOracle(executionConfig: ExecutionConfig, graph: LoamGraph): DirOracle[Tool] = {
    implicit val toolsCanBeASimplePaths = new LoamToolBox.ToolsCanBeASimplePaths(graph)
      
    val invokesLsTools = graph.tools.filter(_.isInstanceOf[InvokesLsTool])
      
    DirOracle.For(executionConfig, _.workerDir, invokesLsTools)
  }
}
