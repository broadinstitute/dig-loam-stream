package loamstream.loam

import java.net.URI
import java.nio.file.Path

import loamstream.loam.LoamGraph.StoreLocation
import loamstream.model.Tool.{AllStores, InputsAndOutputs}
import loamstream.model.{Store, Tool}
import loamstream.model.execute.ExecutionEnvironment
import loamstream.util.{Equivalences, Maps}
import loamstream.util.Traversables

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
      storeLocations = Map.empty,
      storeProducers = Map.empty,
      storeConsumers = Map.empty,
      keysSameSets = Equivalences.empty,
      keysSameLists = Equivalences.empty,
      workDirs = Map.empty,
      executionEnvironments = Map.empty)
  }
}

/** The graph of all Loam stores and tools and their relationships */
final case class LoamGraph(
    stores: Set[Store.Untyped],
    tools: Set[Tool],
    toolInputs: Map[Tool, Set[Store.Untyped]],
    toolOutputs: Map[Tool, Set[Store.Untyped]],
    inputStores: Set[Store.Untyped],
    storeLocations: Map[Store.Untyped, StoreLocation],
    storeProducers: Map[Store.Untyped, Tool],
    storeConsumers: Map[Store.Untyped, Set[Tool]],
    keysSameSets: Equivalences[LoamStoreKeySlot],
    keysSameLists: Equivalences[LoamStoreKeySlot],
    workDirs: Map[Tool, Path],
    executionEnvironments: Map[Tool, ExecutionEnvironment]) {

  /** Returns graph with store added */
  def withStore(store: Store.Untyped): LoamGraph = copy(stores = stores + store)

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

    import Maps.Implicits._

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

  /** Returns graph with store marked as input store */
  def withStoreAsInput(store: Store.Untyped): LoamGraph = copy(inputStores = inputStores + store)

  /** Returns graph with store location (path or URI) added */
  def withStoreLocation(store: Store.Untyped, location: StoreLocation): LoamGraph = {
    copy(storeLocations = storeLocations + (store -> location))
  }

  /** Returns graph with key sets equivalence added */
  def withKeysSameSet(slot1: LoamStoreKeySlot, slot2: LoamStoreKeySlot): LoamGraph = {
    copy(keysSameSets = keysSameSets.withTheseEqual(slot1, slot2))
  }

  /** Returns graph with key lists (sets implied) equivalence added */
  def withKeysSameList(slot1: LoamStoreKeySlot, slot2: LoamStoreKeySlot): LoamGraph = {
    copy(
      keysSameSets = keysSameSets.withTheseEqual(slot1, slot2),
      keysSameLists = keysSameLists.withTheseEqual(slot1, slot2))
  }

  /** True if slots have same key set */
  def areSameKeySets(slot1: LoamStoreKeySlot, slot2: LoamStoreKeySlot): Boolean = {
    keysSameSets.theseAreEqual(slot1, slot2)
  }

  /** True if slots have same key list */
  def areSameKeyLists(slot1: LoamStoreKeySlot, slot2: LoamStoreKeySlot): Boolean = {
    keysSameLists.theseAreEqual(slot1, slot2)
  }

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

  /** Whether store has a Path associated with it */
  def hasPath(store: Store.Untyped): Boolean = {
    storeLocations.get(store).exists(_.isInstanceOf[StoreLocation.PathLocation])
  }

  /** Optionally the path associated with a store */
  def pathOpt(store: Store.Untyped): Option[Path] =
  storeLocations.get(store) match {
    case Some(StoreLocation.PathLocation(path)) => Some(path)
    case _ => None
  }

  /** Whether store has a Path associated with it */
  def hasUri(store: Store.Untyped): Boolean = {
    storeLocations.get(store).exists(_.isInstanceOf[StoreLocation.UriLocation])
  }

  /** Optionally the URI associated with a store */
  def uriOpt(store: Store.Untyped): Option[URI] = {
    storeLocations.get(store).collect {
      case StoreLocation.UriLocation(uri) => uri
    }
  }

  /** Optionally, the work directory of a tool */
  def workDirOpt(tool: Tool): Option[Path] = workDirs.get(tool)

  /** Optionally, the execution environment of a tool */
  def executionEnvironmentOpt(tool: Tool): Option[ExecutionEnvironment] = executionEnvironments.get(tool)

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

  private def storesFor(tool: Tool)(storeMap: Map[Tool, Set[Store.Untyped]]): Set[Store.Untyped] = {
    storeMap.getOrElse(tool, Set.empty)
  }

  private def inputsFor(tool: Tool): Set[Store.Untyped] = storesFor(tool)(toolInputs)

  private def outputsFor(tool: Tool): Set[Store.Untyped] = storesFor(tool)(toolOutputs)

  /** Adds input stores to tool
    *
    * Assuming tool and stores are already part of the graph. If stores were output stores, they will no longer be.
    */
  def withInputStores(tool: Tool, stores: Set[Store.Untyped]): LoamGraph = {
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
  def withOutputStores(tool: Tool, stores: Set[Store.Untyped]): LoamGraph = {
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

  def without(toolsToExclude: Set[Tool]): LoamGraph = containingOnly(tools -- toolsToExclude)
  
  private def containingOnly(toolsToKeep: Set[Tool]): LoamGraph = {

    if(toolsToKeep == tools) { this }
    else {
      type UStore = Store.Untyped
      
      import Traversables.Implicits._
      
      val retainedInputs: Set[UStore] = toolsToKeep.flatMap(toolInputs(_))
      val retainedOutputs: Set[UStore] = toolsToKeep.flatMap(toolOutputs(_))
      
      val retainedToolsToInputs: Map[Tool, Set[UStore]] = toolsToKeep.mapTo(toolInputs(_))
      val retainedToolsToOutputs: Map[Tool, Set[UStore]] = toolsToKeep.mapTo(toolOutputs(_))
  
      val retainedInputStores: Set[UStore] = inputStores.filter(retainedInputs.contains)
      
      val retainedStores = retainedInputStores ++ retainedInputs ++ retainedOutputs
      
      import Maps.Implicits._
      
      val retainedStoreLocations = storeLocations.filterKeys(retainedStores)
      val retainedStoreProducers = storeProducers.filterKeys(retainedStores).filterValues(toolsToKeep)
      val retainedStoreConsumers = {
        storeConsumers.filterKeys(retainedStores).strictMapValues(_.filter(toolsToKeep)).filterValues(_.nonEmpty)
      }
      val retainedWorkDirs = workDirs.filterKeys(toolsToKeep)
      val retainedExecutionEnvironments = executionEnvironments.filterKeys(toolsToKeep)
  
      val retainedKeysSameSets = keysSameSets //TODO: correct?
      val retainedKeysSameLists = keysSameLists //TODO: correct?
      
      LoamGraph(
          stores = retainedStores,
          tools = toolsToKeep,
          toolInputs = retainedToolsToInputs,
          toolOutputs = retainedToolsToOutputs,
          inputStores = retainedInputStores,
          storeLocations = retainedStoreLocations,
          storeProducers = retainedStoreProducers,
          storeConsumers = retainedStoreConsumers,
          keysSameSets = retainedKeysSameSets,
          keysSameLists = retainedKeysSameLists,
          workDirs = retainedWorkDirs,
          executionEnvironments = retainedExecutionEnvironments)
    }
  }
}
