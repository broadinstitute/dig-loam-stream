package loamstream.loam

import java.nio.file.Path

import loamstream.LEnv
import loamstream.loam.LoamGraph.StoreEdge

/**
  * LoamStream
  * Created by oliverr on 6/10/2016.
  */
object LoamGraph {

  trait StoreEdge

  object StoreEdge {

    case class PathEdge(path: Path) extends StoreEdge

    case class PathKeyEdge(key: LEnv.Key[Path]) extends StoreEdge

    case class ToolEdge(tool: ToolBuilder) extends StoreEdge

  }

  def empty: LoamGraph = LoamGraph(Set.empty, Set.empty, Map.empty, Map.empty, Map.empty, Map.empty)
}

case class LoamGraph(stores: Set[StoreBuilder], tools: Set[ToolBuilder],
                     toolInputs: Map[ToolBuilder, Set[StoreBuilder]],
                     toolOutputs: Map[ToolBuilder, Set[StoreBuilder]],
                     storeSources: Map[StoreBuilder, StoreEdge],
                     storeSinks: Map[StoreBuilder, Set[StoreEdge]]) {

  def +(store: StoreBuilder): LoamGraph = copy(stores = stores + store)

  def +(tool: ToolBuilder): LoamGraph =
    if (!tools(tool)) {
      val toolStores = tool.stores.toSet
      val toolInputStores = toolStores.filter(storeSources.contains)
      val toolOutputStores = toolStores -- toolInputStores
      val toolEdge = StoreEdge.ToolEdge(tool)
      val outputsWithSource = toolOutputStores.map(store => store -> toolEdge)
      val storeSinksNew =
        toolInputStores.map(store => store -> (storeSinks.getOrElse(store, Set.empty) + toolEdge))
      copy(tools = tools + tool, toolInputs = toolInputs + (tool -> toolInputStores),
        toolOutputs = toolOutputs + (tool -> toolOutputStores), storeSources = storeSources ++ outputsWithSource,
        storeSinks = storeSinks ++ storeSinksNew)
    } else {
      this
    }

  def withStoreSource(store: StoreBuilder, source: StoreEdge): LoamGraph =
    copy(storeSources = storeSources + (store -> source))

  def withStoreSink(store: StoreBuilder, sink: StoreEdge): LoamGraph =
    copy(storeSinks = storeSinks + (store -> (storeSinks.getOrElse(store, Set.empty) + sink)))

  def storeProducers(store: StoreBuilder): Option[ToolBuilder] = storeSources.get(store).flatMap({
    case StoreEdge.ToolEdge(tool) => Some(tool)
    case _ => None
  })

  def storeConsumers(store: StoreBuilder): Set[ToolBuilder] = storeSinks.getOrElse(store, Set.empty).flatMap({
    case StoreEdge.ToolEdge(tool) => Some(tool)
    case _ => None
  })

  def toolsPreceding(tool: ToolBuilder): Set[ToolBuilder] =
    toolInputs.getOrElse(tool, Set.empty).flatMap(storeSources.get).collect({
      case StoreEdge.ToolEdge(toolPreceding) => toolPreceding
    })

  def toolsSucceeding(tool: ToolBuilder): Set[ToolBuilder] =
    toolOutputs.getOrElse(tool, Set.empty).flatMap(storeConsumers)
}
