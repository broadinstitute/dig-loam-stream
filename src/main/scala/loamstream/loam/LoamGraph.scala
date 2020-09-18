package loamstream.loam

import java.net.URI
import java.nio.file.Path

import loamstream.loam.LoamGraph.StoreLocation
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.Tool.AllStores
import loamstream.model.Tool.InputsAndOutputs
import loamstream.util.Sequence
import loamstream.util.Traversables
import loamstream.util.BiMap
import loamstream.model.execute.Settings

/** The graph of all Loam stores and tools and their relationships */
object LoamGraph {

  /** The location of a store */
  sealed trait StoreLocation

  /** The location of a store */
  object StoreLocation {

    /** Store location based on a Path */
    final case class PathLocation(path: Path) extends StoreLocation {
      override def toString: String = path.toString
    }

    /** Store location based on a URI */
    final case class UriLocation(uri: URI) extends StoreLocation {
      override def toString: String = uri.toString
    }

  }

  /** An empty graph */
  lazy val empty: LoamGraph = {
    LoamGraph(
      stores = Set.empty,
      tools = Set.empty,
      toolInputs = Map.empty,
      toolOutputs = Map.empty,
      inputStores = Set.empty,
      storeProducers = Map.empty,
      storeConsumers = Map.empty,
      workDirs = Map.empty,
      toolSettings = Map.empty,
      namedTools = BiMap.empty)
  }
  
  private[this] val toolNameSequence: Sequence[Int] = Sequence()
  
  val autogeneratedToolNamePrefix: String = "$anon$tool$name"
  
  private def autoGeneratedToolName(): String = s"${autogeneratedToolNamePrefix}-${toolNameSequence.next()}"
  
  def isAutogenerated(name: String): Boolean = name.startsWith(autogeneratedToolNamePrefix) 
}

/** The graph of all Loam stores and tools and their relationships */
final case class LoamGraph(
    stores: Set[Store],
    tools: Set[Tool],
    toolInputs: Map[Tool, Set[Store]],
    toolOutputs: Map[Tool, Set[Store]],
    inputStores: Set[Store],
    storeProducers: Map[Store, Tool],
    storeConsumers: Map[Store, Set[Tool]],
    //TODO: put "metadata" (work dirs, tool names, environments) that's not directly related to tool-store topologies
    //somewhere else?  For now, following the established pattern.  -Clint Nov 9, 2017
    workDirs: Map[Tool, Path],
    toolSettings: Map[Tool, Settings],
    namedTools: BiMap[Tool, String]) {

  /** Returns graph with store added */
  def withStore(store: Store): LoamGraph = copy(stores = stores + store)

  /** Returns graph with tool added */
  def withTool(tool: Tool, scriptContext: LoamScriptContext): LoamGraph = {
    if (tools(tool)) {
      if (workDirs.contains(tool) && workDirs(tool) == scriptContext.workDir) {
        this
      } else {
        copy(workDirs = workDirs + (tool -> scriptContext.workDir))
      }
    } else {
      val (toolInputStores, toolOutputStores) = tool.defaultStores match {
        case AllStores(toolStores) => {
          def isInputStore(s: Store) = inputStores.contains(s) || storeProducers.contains(s)
          
          val toolInputStores = toolStores.filter(isInputStore)

          val toolOutputStores = toolStores -- toolInputStores
          
          (toolInputStores, toolOutputStores)
        }
        case InputsAndOutputs(inputs, outputs) => (inputs.toSet, outputs.toSet)
      }

      import Traversables.Implicits._
      
      val outputsWithProducer: Map[Store, Tool] = toolOutputStores.mapTo(_ => tool)
      
      val storeConsumersNew: Map[Store, Set[Tool]] = toolInputStores.mapTo { store => 
        val existingConsumers = storeConsumers.getOrElse(store, Set.empty)
        
        existingConsumers + tool
      }

      val result = this.copy(
        tools = tools + tool,
        toolInputs = toolInputs + (tool -> toolInputStores),
        toolOutputs = toolOutputs + (tool -> toolOutputStores),
        storeProducers = storeProducers ++ outputsWithProducer,
        storeConsumers = storeConsumers ++ storeConsumersNew,
        workDirs = workDirs + (tool -> scriptContext.workDir),
        toolSettings = toolSettings + (tool -> scriptContext.settings)
      )
      
      result.withToolName(tool, LoamGraph.autoGeneratedToolName())
    }
  }

  /** Returns a new graph with a given tool replaced by another specified tool
   *  along with its dependencies
   */
  def updateTool(existing: Tool, replacement: Tool): LoamGraph = {
    val replace: Tool => Tool = { tool =>
      if (tool.id == existing.id) { replacement } else { tool }
    }

    import loamstream.util.Maps.Implicits._

    copy(
      tools = tools.map(replace),
      toolInputs = toolInputs.mapKeys(replace),
      toolOutputs = toolOutputs.mapKeys(replace),
      storeProducers = storeProducers.mapValues(replace),
      storeConsumers = storeConsumers.strictMapValues(_.map(replace)),
      workDirs = workDirs.mapKeys(replace),
      toolSettings = toolSettings.mapKeys(replace),
      namedTools = namedTools.mapKeys(replace)
    )
  }

  /** Returns graph with store marked as input store */
  def withStoreAsInput(store: Store): LoamGraph = copy(inputStores = inputStores + store)

  /** Tools that produce a store consumed by this tool */
  def toolsPreceding(tool: Tool): Set[Tool] = {
    toolInputs.getOrElse(tool, Set.empty).flatMap(storeProducers.get)
  }

  /** Tools that consume a store produced by this tool */
  def toolsSucceeding(tool: Tool): Set[Tool] = {
    toolOutputs.getOrElse(tool, Set.empty).flatMap(storeConsumers.getOrElse(_, Set.empty))
  }

  /** All tools with no preceeding tools */
  def initialTools: Set[Tool] = tools.filter(toolsPreceding(_).isEmpty)

  /** All tools with no succeeding tools */
  def finalTools: Set[Tool] = tools.filter(toolsSucceeding(_).isEmpty)

  /** Optionally, the work directory of a tool */
  def workDirOpt(tool: Tool): Option[Path] = workDirs.get(tool)

  /** Optionally, the execution environment of a tool */
  def settingsOpt(tool: Tool): Option[Settings] = toolSettings.get(tool)

  /** Ranks for all tools: zero for final tools; for all others one plus maximum of rank of succeeding tools */
  def ranks: Map[Tool, Int] = {
    val initialRanks: Map[Tool, Int] = tools.map(tool => (tool, 0)).toMap

    tools.foldLeft(initialRanks) { (ranks, tool) =>
      def rankFor(tool: Tool): Int = ranks.getOrElse(tool, 0)

      def succeedingToolRanks(tool: Tool): Set[Int] = toolsSucceeding(tool).map(rankFor).map(_ + 1)

      val newRankEstimate = (succeedingToolRanks(tool) + 0).max

      if (newRankEstimate == ranks(tool)) {
        ranks
      } else {
        ranks + (tool -> newRankEstimate)
      }
    }
  }

  private def storesFor(tool: Tool)(storeMap: Map[Tool, Set[Store]]): Set[Store] = {
    storeMap.getOrElse(tool, Set.empty)
  }

  private def inputsFor(tool: Tool): Set[Store] = storesFor(tool)(toolInputs)

  private def outputsFor(tool: Tool): Set[Store] = storesFor(tool)(toolOutputs)

  /** Adds input stores to tool
    *
    * Assuming tool and stores are already part of the graph. If stores were output stores, they will no longer be.
    */
  def withInputStores(tool: Tool, stores: Set[Store]): LoamGraph = {
    val toolInputsNew = toolInputs + (tool -> (inputsFor(tool) ++ stores))

    val toolOutputsNew = toolOutputs + (tool -> (outputsFor(tool) -- stores))

    //Remove only at most `stores.size` keys from `storeProducers`.  Previously, this code relied on calling
    //`storeProducers.filterNot`, which traversed the entire storeProducers Map and ran in O(nStores) time.
    //The current approach is faster since for non-trivial pipelines, the number of keys to remove from 
    //storeProducers is usually *much* smaller than the total number of stores, and removing a key from a map is
    //fast, either O(1) or O(log nStores).
    val storeProducersKeysToRemove: Iterable[Store] = {
      stores.map { s => s -> storeProducers.get(s) }.collect { case (s, Some(producer)) if producer == tool => s }
    }
    
    val storeProducersNew = storeProducers -- storeProducersKeysToRemove

    val storeConsumersNew = {
      storeConsumers ++ stores.map(store => (store, storeConsumers.getOrElse(store, Set.empty) + tool))
    }

    copy(
      toolInputs = toolInputsNew,
      toolOutputs = toolOutputsNew,
      storeProducers = storeProducersNew,
      storeConsumers = storeConsumersNew)
  }

  /** Adds output stores to tool
    *
    * Assuming tool and stores are already part of the graph. If stores were input stores, they will no longer be.
    */
  def withOutputStores(tool: Tool, stores: Set[Store]): LoamGraph = {
    val toolInputsNew = toolInputs + (tool -> (inputsFor(tool) -- stores))

    val toolOutputsNew = toolOutputs + (tool -> (outputsFor(tool) ++ stores))

    val storeProducersNew = storeProducers ++ stores.map(store => store -> tool)

    val storeConsumersNew =
      storeConsumers ++ stores.map(store => (store, storeConsumers.getOrElse(store, Set.empty) - tool))

    copy(
      toolInputs = toolInputsNew,
      toolOutputs = toolOutputsNew,
      storeProducers = storeProducersNew,
      storeConsumers = storeConsumersNew)
  }

  def nameOf(t: Tool): Option[String] = namedTools.get(t)
  
  def withToolName(tool: Tool, tagName: String): LoamGraph = {
    //TODO: Throw here, or elsewhere?  Make this a loam-compilation-time error another way?
    require(!namedTools.containsValue(tagName), s"Tool tag name '$tagName' must be unique.")
    
    val tuple = tool -> tagName
    
    def newGraphWithNamedTool: LoamGraph = copy(namedTools = (namedTools + tuple))

    def newGraphWithRenamedTool(currentName: String): LoamGraph = {
      copy(namedTools = (namedTools.withoutValue(currentName) + tuple))
    }
    
    import LoamGraph.isAutogenerated
    
    nameOf(tool) match {
      case None => newGraphWithNamedTool
      case Some(currentName) if isAutogenerated(currentName) => newGraphWithRenamedTool(currentName)
      //TODO: Throw here, or elsewhere?  Make this a loam-compilation-time error another way?
      case _ => sys.error(s"Tool '$tool' is already tagged as ${nameOf(tool).get}")
    }
  }
  
  def without(toolsToExclude: Set[Tool]): LoamGraph = containingOnly(tools -- toolsToExclude)
  
  private def containingOnly(toolsToKeep: Set[Tool]): LoamGraph = {

    if(toolsToKeep == tools) { this }
    else {
      import loamstream.util.Traversables.Implicits._
      
      val retainedInputs: Set[Store] = toolsToKeep.flatMap(toolInputs(_))
      val retainedOutputs: Set[Store] = toolsToKeep.flatMap(toolOutputs(_))
      
      val retainedToolsToInputs: Map[Tool, Set[Store]] = toolsToKeep.mapTo(toolInputs(_))
      val retainedToolsToOutputs: Map[Tool, Set[Store]] = toolsToKeep.mapTo(toolOutputs(_))
  
      val retainedInputStores: Set[Store] = inputStores.filter(retainedInputs)
      
      val retainedStores = retainedInputStores ++ retainedInputs ++ retainedOutputs
      
      import loamstream.util.Maps.Implicits._
      
      val retainedStoreProducers = storeProducers.filterKeys(retainedStores).filterValues(toolsToKeep)
      val retainedStoreConsumers = {
        storeConsumers.filterKeys(retainedStores).strictMapValues(_.filter(toolsToKeep)).filterValues(_.nonEmpty)
      }
      val retainedWorkDirs = workDirs.filterKeys(toolsToKeep)
      val retainedExecutionEnvironments = toolSettings.filterKeys(toolsToKeep)
  
      val retainedNamedTools = namedTools.filterKeys(toolsToKeep)
      
      LoamGraph(
          stores = retainedStores,
          tools = toolsToKeep,
          toolInputs = retainedToolsToInputs,
          toolOutputs = retainedToolsToOutputs,
          inputStores = retainedInputStores,
          storeProducers = retainedStoreProducers,
          storeConsumers = retainedStoreConsumers,
          workDirs = retainedWorkDirs,
          toolSettings = retainedExecutionEnvironments,
          namedTools = retainedNamedTools)
    }
  }
}
