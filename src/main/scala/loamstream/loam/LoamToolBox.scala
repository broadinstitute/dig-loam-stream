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
    lazy val newJobParams = new NewJobParams(graph, oracle, tool)
    
    import newJobParams.commandLineJob
    import newJobParams.nativeJob
    
    tool match {
      case cmdTool: LoamCmdTool => Some(commandLineJob(cmdTool.commandLine))
      case invokesLs: InvokesLsTool => {
        val commandLineTokens = makeWorkerJobCommandLineTokens(graph, invokesLs, oracle)
        
        Some(commandLineJob(commandLineTokens.mkString(" ")))
      }
      case nativeTool: NativeTool => Some(nativeJob(nativeTool))
      case t => {
        warn(s"Not mapping tool with unknown type: $t")
        
        None
      }
    }
  }

  private final class NewJobParams(graph: LoamGraph, oracle: DirOracle[Tool], tool: Tool) {
    
    private val workDir: Path = graph.workDirOpt(tool).getOrElse(Paths.get("."))

    private val settings: Settings = graph.settingsOpt(tool).getOrElse(LocalSettings)

    private val dependencyJobs = toJobs(graph, oracle, graph.toolsPreceding(tool))
    private def successorJobs = toJobs(graph, oracle, graph.toolsSucceeding(tool))

    private val inputs = LoamToolBox.inputsFor(graph, tool, client)
    private val outputs = LoamToolBox.outputsFor(graph, tool, client)

    private val toolNameOpt = graph.nameOf(tool)
    
    def commandLineJob(commandLine: String, initialSettings: Settings = settings): CommandLineJob = CommandLineJob(
      commandLineString = commandLine, 
      workDir = workDir, 
      initialSettings = initialSettings, 
      dependencies = dependencyJobs,
      successorsFn = () => successorJobs, 
      inputs = inputs,
      outputs = outputs, 
      nameOpt = toolNameOpt)
      
    def nativeJob(nativeTool: NativeTool): NativeJob = NativeJob(
        body = nativeTool.body,
        initialSettings = LocalSettings, 
        dependencies = dependencyJobs,
        successorsFn = () => successorJobs, 
        inputs = inputs,
        outputs = outputs, 
        nameOpt = toolNameOpt)
  }
  
  private def makeWorkerJobCommandLineTokens(
      graph: LoamGraph, 
      invokesLs: InvokesLsTool, 
      oracle: DirOracle[Tool]): Seq[String] = {
    
    val lsSettings = invokesLs.scriptContext.lsSettings
    
    val jvmArgs = lsSettings.jvmArgs
    val cliConfig = lsSettings.cliConfig
    
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
  
  private def inputsFor(
      graph: LoamGraph, 
      tool: Tool, 
      client: Option[CloudStorageClient]): Set[DataHandle] = handlesFor(graph.toolInputs(tool), client)
  
  private def outputsFor(
      graph: LoamGraph, 
      tool: Tool, 
      client: Option[CloudStorageClient]): Set[DataHandle] = handlesFor(graph.toolOutputs(tool), client)
  
  private def handlesFor(stores: Set[Store], client: Option[CloudStorageClient]): Set[DataHandle] = {
      def pathOrUriToOutput(store: Store): Option[DataHandle] = {
        store.pathOpt.orElse(store.uriOpt).map {
          case path: Path => DataHandle.PathHandle(path)
          case uri: URI   => DataHandle.GcsUriHandle(uri, client)
        }
      }
      
      stores.flatMap(pathOrUriToOutput)
    }
}
