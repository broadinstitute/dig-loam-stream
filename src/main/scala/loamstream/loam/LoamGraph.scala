package loamstream.loam

import java.net.URI
import java.nio.file.Path

import loamstream.loam.LoamGraph.StoreLocation
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.Tool.AllStores
import loamstream.model.Tool.InputsAndOutputs
import loamstream.model.execute.Environment

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
      executionEnvironments = Map.empty,
      namedTools = Map.empty)
  }
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
    executionEnvironments: Map[Tool, Environment],
    namedTools: Map[String, Tool]) {

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
        case AllStores(toolStores) =>
          val toolInputStores =
            toolStores.filter(store => inputStores.contains(store) || storeProducers.contains(store))
          val toolOutputStores = toolStores -- toolInputStores
          (toolInputStores, toolOutputStores)
        case InputsAndOutputs(inputs, outputs) => (inputs.toSet, outputs.toSet)
      }
      val outputsWithProducer = toolOutputStores.map(store => store -> tool)
      val storeConsumersNew =
        toolInputStores.map(store => store -> (storeConsumers.getOrElse(store, Set.empty) + tool))

      copy(
        tools = tools + tool,
        toolInputs = toolInputs + (tool -> toolInputStores),
        toolOutputs = toolOutputs + (tool -> toolOutputStores),
        storeProducers = storeProducers ++ outputsWithProducer,
        storeConsumers = storeConsumers ++ storeConsumersNew,
        workDirs = workDirs + (tool -> scriptContext.workDir),
        executionEnvironments = executionEnvironments + (tool -> scriptContext.executionEnvironment)
      )
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
      storeProducers = storeProducers.mapValues(replace).view.force,
      storeConsumers = storeConsumers.mapValues(_.map(replace)).view.force,
      workDirs = workDirs.mapKeys(replace),
      executionEnvironments = executionEnvironments.mapKeys(replace)
    )
  }
  
  /*def updateStore(existing: Store, replacement: Store): LoamGraph = {
    val replace: Store => Store = { store =>
      if (store.id == existing.id) { replacement } else { store }
    }

    import loamstream.util.Maps.Implicits._

    copy(
      stores = stores.map(replace),
      toolInputs = toolInputs.strictMapValues(_.map(replace)),
      toolOutputs = toolOutputs.strictMapValues(_.map(replace)),
      inputStores = inputStores.map(replace),
      storeProducers = storeProducers.mapKeys(replace),
      storeConsumers = storeConsumers.mapKeys(replace))
  }*/

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
  def executionEnvironmentOpt(tool: Tool): Option[Environment] = executionEnvironments.get(tool)

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

    val storeProducersNew = storeProducers.filterNot {
      case (store, producer) => stores(store) && producer == tool
    }

    val storeConsumersNew =
      storeConsumers ++ stores.map(store => (store, storeConsumers.getOrElse(store, Set.empty) + tool))

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

  def nameOf(t: Tool): Option[String] = namedTools.collectFirst { case (n, namedTool) if namedTool == t => n }
  
  def withToolName(tool: Tool, name: String): LoamGraph = {
    //TODO: Throw here, or elsewhere?  Make this a loam-compilation-time error another way?
    if(namedTools.contains(name)) {
      throw new Exception(s"Tool name '$name' must be unique.")
    }
    
    if(namedTools.values.toSet.contains(tool)) {
      throw new Exception(s"Tool '$tool' is already named ${nameOf(tool).get}")
    }
    
    copy(namedTools = namedTools + (name -> tool))
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
      val retainedExecutionEnvironments = executionEnvironments.filterKeys(toolsToKeep)
  
      val retainedNamedTools = namedTools.filterValues(toolsToKeep)
      
      LoamGraph(
          stores = retainedStores,
          tools = toolsToKeep,
          toolInputs = retainedToolsToInputs,
          toolOutputs = retainedToolsToOutputs,
          inputStores = retainedInputStores,
          storeProducers = retainedStoreProducers,
          storeConsumers = retainedStoreConsumers,
          workDirs = retainedWorkDirs,
          executionEnvironments = retainedExecutionEnvironments,
          namedTools = retainedNamedTools)
    }
  }
}
