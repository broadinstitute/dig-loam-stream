package loamstream.loam

import java.net.URI
import java.nio.file.Path

import loamstream.loam.LoamGraph.StoreLocation
import loamstream.loam.LoamTool.{AllStores, InputsAndOutputs}
import loamstream.model.execute.ExecutionEnvironment
import loamstream.util.Equivalences

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
  def empty: LoamGraph = {
    LoamGraph(
      Set.empty,
      Set.empty,
      Map.empty,
      Map.empty,
      Set.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Equivalences.empty,
      Equivalences.empty,
      Map.empty,
      Map.empty)
  }
}

/** The graph of all Loam stores and tools and their relationships */
final case class LoamGraph(stores: Set[LoamStore.Untyped],
                           tools: Set[LoamTool],
                           toolInputs: Map[LoamTool, Set[LoamStore.Untyped]],
                           toolOutputs: Map[LoamTool, Set[LoamStore.Untyped]],
                           inputStores: Set[LoamStore.Untyped],
                           storeLocations: Map[LoamStore.Untyped, StoreLocation],
                           storeProducers: Map[LoamStore.Untyped, LoamTool],
                           storeConsumers: Map[LoamStore.Untyped, Set[LoamTool]],
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

  /** Returns graph with store marked as input store */
  def withStoreAsInput(store: LoamStore.Untyped): LoamGraph = copy(inputStores = inputStores + store)

  /** Returns graph with store location (path or URI) added */
  def withStoreLocation(store: LoamStore.Untyped, location: StoreLocation): LoamGraph = {
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
  def toolsPreceding(tool: LoamTool): Set[LoamTool] = {
    toolInputs.getOrElse(tool, Set.empty).flatMap(storeProducers.get)
  }


  /** Tools that consume a store produced by this tool */
  def toolsSucceeding(tool: LoamTool): Set[LoamTool] = {
    toolOutputs.getOrElse(tool, Set.empty).flatMap(storeConsumers.getOrElse(_, Set.empty))
  }

  /** All tools with no preceeding tools */
  def initialTools: Set[LoamTool] = tools.filter(toolsPreceding(_).isEmpty)

  /** All tools with no succeeding tools */
  def finalTools: Set[LoamTool] = tools.filter(toolsSucceeding(_).isEmpty)

  /** Whether store has a Path associated with it */
  def hasPath(store: LoamStore.Untyped): Boolean = {
    storeLocations.get(store).exists(_.isInstanceOf[StoreLocation.PathLocation])
  }

  /** Optionally the path associated with a store */
  def pathOpt(store: LoamStore.Untyped): Option[Path] =
  storeLocations.get(store) match {
    case Some(StoreLocation.PathLocation(path)) => Some(path)
    case _ => None
  }

  /** Whether store has a Path associated with it */
  def hasUri(store: LoamStore.Untyped): Boolean = {
    storeLocations.get(store).exists(_.isInstanceOf[StoreLocation.UriLocation])
  }

  /** Optionally the URI associated with a store */
  def uriOpt(store: LoamStore.Untyped): Option[URI] = {
    storeLocations.get(store).collect {
      case StoreLocation.UriLocation(uri) => uri
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

  /** Adds input stores to tool
    *
    * Assuming tool and stores are already part of the graph. If stores were output stores, they will no longer be.
    */
  def withInputStores(tool: LoamTool, stores: Set[LoamStore.Untyped]): LoamGraph = {
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
  def withOutputStores(tool: LoamTool, stores: Set[LoamStore.Untyped]): LoamGraph = {
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

}
