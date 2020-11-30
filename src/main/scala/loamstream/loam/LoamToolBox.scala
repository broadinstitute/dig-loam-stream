package loamstream.loam

import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths

import loamstream.conf.ExecutionConfig
import loamstream.googlecloud.CloudStorageClient
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.execute.Executable
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.Settings
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.NativeJob
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.DirOracle
import loamstream.util.DirTree.DirNode
import loamstream.util.Loggable
import loamstream.util.jvm.SysPropNames

/**
 * LoamStream
 * 
 * Created by oliverr on 6/21/2016.
 * 
 * Turns a LoamGraph into an Executable (a collection of jobs)
 */
final class LoamToolBox(client: Option[CloudStorageClient] = None) extends Loggable {

  @volatile private[this] var loamJobs: java.util.Map[Tool, JobNode] = new java.util.HashMap

  private[this] val lock = new AnyRef

  private def clear(): Unit = lock.synchronized { 
    loamJobs.clear()
    
    loamJobs = null
  }
  
  def createExecutable(graph: LoamGraph, executionConfig: ExecutionConfig = ExecutionConfig.default): Executable = {
    val oracle = LoamToolBox.makeDirOracle(executionConfig, graph)
    
    try {
      Executable(toJobs(graph, oracle, graph.finalTools))
    } finally {
      clear()
    }
  }

  private[loam] def toJobs(graph: LoamGraph, oracle: DirOracle[Tool], finalTools: Iterable[Tool]): Set[JobNode] = {
    finalTools.flatMap(getJob(graph, oracle)).toSet
  }
  
  def getJob(graph: LoamGraph, oracle: DirOracle[Tool])(tool: Tool): Option[JobNode] = lock.synchronized {
    if(loamJobs.containsKey(tool)) { Option(loamJobs.get(tool)) }
    else { 
      val newJobOpt = newJob(graph, oracle, tool)
        
      newJobOpt.foreach { job =>
        loamJobs.put(tool, job)
      }
      
      newJobOpt
    }
    
    /*loamJobs.get(tool) match {
      case s @ Some(_) => s
      case None => {
        val newJobOpt = newJob(graph, oracle, tool)
        
        newJobOpt.foreach { job =>
          loamJobs += tool -> job
          
          job
        }
        
        newJobOpt
      }
    }*/
  }
  
  private[loam] def newJob(graph: LoamGraph, oracle: DirOracle[Tool], tool: Tool): Option[JobNode] = {
    lazy val newJobParams = new NewJobParams(graph, oracle, tool)
    
    import newJobParams.commandLineJob
    import newJobParams.nativeJob
    import LoamToolBox.makeWorkerJobCommandLineTokens
    
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
    
    //private val workDir: Path = graph.workDirOpt(tool).getOrElse(Paths.get("."))

    //private val settings: Settings = graph.settingsOpt(tool).getOrElse(LocalSettings)

    //private val dependencyJobs = toJobs(graph, oracle, graph.toolsPreceding(tool))
    //private def successorJobs = toJobs(graph, oracle, graph.toolsSucceeding(tool))

    //private val inputs = LoamToolBox.inputsFor(graph, tool, client)
    //private val outputs = LoamToolBox.outputsFor(graph, tool, client)

    //private val toolNameOpt = graph.nameOf(tool)
    
    def commandLineJob(commandLine: String, initialSettings: Settings = graph.settingsOpt(tool).getOrElse(LocalSettings)): CommandLineJob = new CommandLineJob(
      commandLineString = commandLine, 
      //workDir = workDir, 
      initialSettings = initialSettings, 
      name = graph.nameOf(tool).get) {

      override def dependencies: Set[JobNode] = toJobs(graph, oracle, graph.toolsPreceding(tool))
      override def successors: Set[JobNode] = toJobs(graph, oracle, graph.toolsSucceeding(tool))
      
      override def inputs: Set[DataHandle] = LoamToolBox.inputsFor(graph, tool, client)
      override def outputs: Set[DataHandle] = LoamToolBox.outputsFor(graph, tool, client)
    }
      
    def nativeJob(nativeTool: NativeTool): NativeJob = NativeJob(
        body = nativeTool.body,
        initialSettings = LocalSettings, 
        dependencies = toJobs(graph, oracle, graph.toolsPreceding(tool)),
        successorsFn = () => toJobs(graph, oracle, graph.toolsSucceeding(tool)), 
        inputs = LoamToolBox.inputsFor(graph, tool, client),
        outputs = LoamToolBox.outputsFor(graph, tool, client), 
        nameOpt = graph.nameOf(tool))
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
  
  private[loam] def makeWorkerJobCommandLineTokens(
      graph: LoamGraph, 
      invokesLs: InvokesLsTool, 
      oracle: DirOracle[Tool]): Seq[String] = {
    
    val lsSettings = invokesLs.scriptContext.lsSettings
    
    val jvmArgs = lsSettings.jvmArgs
    val cliConfig = lsSettings.cliConfig
    
    cliConfig match {
      case Some(conf) => {
        val workDirOpt = oracle.dirOptFor(invokesLs)
        val drmSystemOpt = invokesLs.scriptContext.config.drmSystem
        val nameOpt = graph.nameOf(invokesLs)
        
        def isUserSpecifiedName = nameOpt.isDefined && !LoamGraph.isAutogenerated(nameOpt.get)
        
        require(workDirOpt.isDefined, s"Couldn't determine work dir for tool $invokesLs")
        require(drmSystemOpt.isDefined, "Child LS instances must be run on a DRM system")
        require(isUserSpecifiedName, "Tools representing child LS instances must have a user-defined tag")
        
        val workDir = workDirOpt.get
        
        val sysprops = Map(
            SysPropNames.loamstreamWorkDir -> workDir.toString,
            SysPropNames.loamstreamExecutionLoamstreamDir -> workDir.toString)
        
        val newConf = {
          conf.
            withBackend(drmSystemOpt.get).
            withIsWorker(true).
            withWorkDir(workDir).
            onlyRun(nameOpt.get) //TODO
        }
        
        invokesLs.preambles ++ jvmArgs.rerunCommandTokens(newConf, sysprops)
      }
      case None => sys.error(
          s"In order to run in --worker mode, LS must be run from the command line, but no CLI config was found")
    }
  }
}
