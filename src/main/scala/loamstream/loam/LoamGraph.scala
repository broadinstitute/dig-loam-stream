package loamstream.loam

import java.net.URI
import java.nio.file.{ Path, Paths }

import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.LoamGraph.StoreEdge.ToolEdge
import loamstream.loam.LoamTool.{ AllStores, InputsAndOutputs }
import loamstream.util.Equivalences
import loamstream.model.execute.ExecutionEnvironment

/** The graph of all Loam stores and tools and their relationships */
object LoamGraph {

  /** A connection between a store and a tool or other consumer or producer */
  trait StoreEdge

  /** A connection between a store and a tool or other consumer or producer */
  object StoreEdge {

    /** A connection between a store and a path */
    final case class PathEdge(path: Path) extends StoreEdge

    /** A connection between a store and a URI */
    final case class UriEdge(uri: URI) extends StoreEdge

    /** A connection between a store and a tool */
    final case class ToolEdge(tool: LoamTool) extends StoreEdge

  }

  /** An empty graph */
  def empty: LoamGraph = {
    LoamGraph(Set.empty, Set.empty, Map.empty, Map.empty, Map.empty, Map.empty,
      Equivalences.empty, Equivalences.empty, Map.empty, Map.empty)
  }
}

/** The graph of all Loam stores and tools and their relationships */
final case class LoamGraph(stores: Set[LoamStore.Untyped],
                           tools: Set[LoamTool],
                           toolInputs: Map[LoamTool, Set[LoamStore.Untyped]],
                           toolOutputs: Map[LoamTool, Set[LoamStore.Untyped]],
                           storeSources: Map[LoamStore.Untyped, StoreEdge],
                           storeSinks: Map[LoamStore.Untyped, Set[StoreEdge]],
                           keysSameSets: Equivalences[LoamStoreKeySlot],
                           keysSameLists: Equivalences[LoamStoreKeySlot],
                           workDirs: Map[LoamTool, Path],
                           executionEnvironments: Map[LoamTool, ExecutionEnvironment]) {

  /** Returns graph with store added */
  def withStore(store: LoamStore.Untyped): LoamGraph = copy(stores = stores + store)

  /** Returns graph with tool added */
  def withTool(tool: LoamTool, scriptContext: LoamScriptContext): LoamGraph = {
    if (tools(tool)) {
      if (workDirs.contains(tool) && workDirs(tool) == scriptContext.workDir) {
        this
      } else {
        copy(workDirs = workDirs + (tool -> scriptContext.workDir))
      }
    } else {
      val (toolInputStores, toolOutputStores) = tool.defaultStores match {
        case AllStores(toolStores) =>
          val inputStores = toolStores.filter(storeSources.contains)
          val outputStores = toolStores -- inputStores
          (inputStores, outputStores)
        case InputsAndOutputs(inputStores, outputStores) => (inputStores.toSet, outputStores.toSet)
      }
      val toolEdge = StoreEdge.ToolEdge(tool)
      val outputsWithSource = toolOutputStores.map(store => store -> toolEdge)
      val storeSinksNew = toolInputStores.map(store => store -> (storeSinks.getOrElse(store, Set.empty) + toolEdge))

      copy(
        tools = tools + tool,
        toolInputs = toolInputs + (tool -> toolInputStores),
        toolOutputs = toolOutputs + (tool -> toolOutputStores),
        storeSources = storeSources ++ outputsWithSource,
        storeSinks = storeSinks ++ storeSinksNew,
        workDirs = workDirs + (tool -> scriptContext.workDir),
        executionEnvironments = executionEnvironments + (tool -> scriptContext.executionEnvironment))
    }
  }

  /** Returns graph with store source (tool or file) added */
  def withStoreSource(store: LoamStore.Untyped, source: StoreEdge): LoamGraph =
    copy(storeSources = storeSources + (store -> source))

  /** Returns graph with store sink (tool or file) added */
  def withStoreSink(store: LoamStore.Untyped, sink: StoreEdge): LoamGraph =
    copy(storeSinks = storeSinks + (store -> (storeSinks.getOrElse(store, Set.empty) + sink)))

  /** Returns graph with key sets equivalence added */
  def withKeysSameSet(slot1: LoamStoreKeySlot, slot2: LoamStoreKeySlot): LoamGraph =
    copy(keysSameSets = keysSameSets.withTheseEqual(slot1, slot2))

  /** Returns graph with key lists (sets implied) equivalence added */
  def withKeysSameList(slot1: LoamStoreKeySlot, slot2: LoamStoreKeySlot): LoamGraph =
    copy(keysSameSets = keysSameSets.withTheseEqual(slot1, slot2),
      keysSameLists = keysSameLists.withTheseEqual(slot1, slot2))

  /** True if slots have same key set */
  def areSameKeySets(slot1: LoamStoreKeySlot, slot2: LoamStoreKeySlot): Boolean =
    keysSameSets.theseAreEqual(slot1, slot2)

  /** True if slots have same key list */
  def areSameKeyLists(slot1: LoamStoreKeySlot, slot2: LoamStoreKeySlot): Boolean =
    keysSameLists.theseAreEqual(slot1, slot2)

  /** Returns the option of a producer (tool) of a store */
  def storeProducerOpt(store: LoamStore.Untyped): Option[LoamTool] = storeSources.get(store).collect {
    case StoreEdge.ToolEdge(tool) => tool
  }

  /** Returns the set of consumers (tools) of a store */
  def storeConsumers(store: LoamStore.Untyped): Set[LoamTool] = storeSinks.getOrElse(store, Set.empty).collect {
    case StoreEdge.ToolEdge(tool) => tool
  }

  /** Tools that produce a store consumed by this tool */
  def toolsPreceding(tool: LoamTool): Set[LoamTool] =
    toolInputs.getOrElse(tool, Set.empty).flatMap(storeSources.get).collect {
      case StoreEdge.ToolEdge(toolPreceding) => toolPreceding
    }

  /** Tools that consume a store produced by this tool */
  def toolsSucceeding(tool: LoamTool): Set[LoamTool] =
    toolOutputs.getOrElse(tool, Set.empty).flatMap(storeConsumers)

  /** All tools with no preceeding tools */
  def initialTools: Set[LoamTool] = tools.filter(toolsPreceding(_).isEmpty)

  /** All tools with no succeeding tools */
  def finalTools: Set[LoamTool] = tools.filter(toolsSucceeding(_).isEmpty)

  private def storeSinkPaths(store: LoamStore.Untyped): Set[Path] = {
    storeSinks.getOrElse(store, Set.empty).collect { case StoreEdge.PathEdge(path) => path }
  }
  
  private def storeSinkUris(store: LoamStore.Untyped): Set[URI] = {
    storeSinks.getOrElse(store, Set.empty).collect { case StoreEdge.UriEdge(uri) => uri }
  }
    
  /** Whether store has a Path associated with it */
  def hasPath(store: LoamStore.Untyped): Boolean = {
    storeSources.get(store) match {
      case Some(StoreEdge.PathEdge(path)) => true
      case _ => storeSinkPaths(store).nonEmpty
    }
  }

  /** Optionally the path associated with a store */
  def pathOpt(store: LoamStore.Untyped): Option[Path] = {
    storeSources.get(store) match {
      case Some(StoreEdge.PathEdge(path)) => Some(path)
      case _ => storeSinkPaths(store).headOption
    }
  }

  /** Whether store has a Path associated with it */
  def hasUri(store: LoamStore.Untyped): Boolean = {
    storeSources.get(store) match {
      case Some(StoreEdge.UriEdge(path)) => true
      case _ => storeSinkUris(store).nonEmpty
    }
  }

  /** Optionally the URI associated with a store */
  def uriOpt(store: LoamStore.Untyped): Option[URI] = {
    storeSources.get(store) match {
      case Some(StoreEdge.UriEdge(uri)) => Some(uri)
      case _ => storeSinkUris(store).headOption
    }
  }

  /** Optionally, the work directory of a tool */
  def workDirOpt(tool: LoamTool): Option[Path] = workDirs.get(tool)
  
  /** Optionally, the execution environment of a tool */
  def executionEnvironmentOpt(tool: LoamTool): Option[ExecutionEnvironment] = executionEnvironments.get(tool)

  /** Ranks for all tools: zero for final tools; for all others one plus maximum of rank of succeeding tools */
  def ranks: Map[LoamTool, Int] = {
    val initialRanks: Map[LoamTool, Int] = tools.map(tool => (tool, 0)).toMap

    tools.foldLeft(initialRanks) { (ranks, tool) =>
      def rankFor(tool: LoamTool): Int = ranks.getOrElse(tool, 0)

      def succeedingToolRanks(tool: LoamTool): Set[Int] = toolsSucceeding(tool).map(rankFor).map(_ + 1)

      val newRankEstimate = (succeedingToolRanks(tool) + 0).max

      if (newRankEstimate == ranks(tool)) {
        ranks
      } else {
        ranks + (tool -> newRankEstimate)
      }
    }
  }

  private def storesFor(tool: LoamTool)(storeMap: Map[LoamTool, Set[LoamStore.Untyped]]): Set[LoamStore.Untyped] = {
    storeMap.getOrElse(tool, Set.empty)
  }

  private def inputsFor(tool: LoamTool): Set[LoamStore.Untyped] = storesFor(tool)(toolInputs)

  private def outputsFor(tool: LoamTool): Set[LoamStore.Untyped] = storesFor(tool)(toolOutputs)

  /**
   * Adds input stores to tool
   *
   * Assuming tool and stores are already part of the graph. If stores were output stores, they will no longer be.
   */
  def withInputStores(tool: LoamTool, stores: Set[LoamStore.Untyped]): LoamGraph = {
    val toolInputsNew = toolInputs + (tool -> (inputsFor(tool) ++ stores))

    val toolOutputsNew = toolOutputs + (tool -> (outputsFor(tool) -- stores))

    val toolEdge = ToolEdge(tool)

    val storeSourcesNew = storeSources.filterNot {
      case (store, edge) => stores(store) && edge == toolEdge
    }

    val storeSinksNew = storeSinks ++ stores.map(store => (store, storeSinks.getOrElse(store, Set.empty) + toolEdge))

    copy(
      toolInputs = toolInputsNew,
      toolOutputs = toolOutputsNew,
      storeSources = storeSourcesNew,
      storeSinks = storeSinksNew)
  }

  /**
   * Adds output stores to tool
   *
   * Assuming tool and stores are already part of the graph. If stores were input stores, they will no longer be.
   */
  def withOutputStores(tool: LoamTool, stores: Set[LoamStore.Untyped]): LoamGraph = {
    val toolInputsNew = toolInputs + (tool -> (inputsFor(tool) -- stores))

    val toolOutputsNew = toolOutputs + (tool -> (outputsFor(tool) ++ stores))

    val toolEdge = ToolEdge(tool)

    val storeSourcesNew = storeSources ++ stores.map(store => (store, toolEdge))

    val storeSinksNew = storeSinks ++ stores.map(store => (store, storeSinks.getOrElse(store, Set.empty) - toolEdge))

    copy(
      toolInputs = toolInputsNew,
      toolOutputs = toolOutputsNew,
      storeSources = storeSourcesNew,
      storeSinks = storeSinksNew)
  }

}
