package loamstream.loam

import java.nio.file.Path

import loamstream.LEnv
import loamstream.loam.LoamGraph.StoreSource

/**
  * LoamStream
  * Created by oliverr on 6/10/2016.
  */
object LoamGraph {

  trait StoreSource

  object StoreSource {

    case class FromPath(path: Path) extends StoreSource

    case class FromPathKey(key: LEnv.Key[Path]) extends StoreSource

    case class FromTool(tool: ToolBuilder) extends StoreSource

  }

  def empty: LoamGraph = LoamGraph(Set.empty, Set.empty, Map.empty, Map.empty, Map.empty, Map.empty)
}

case class LoamGraph(stores: Set[StoreBuilder], tools: Set[ToolBuilder],
                     storeSources: Map[StoreBuilder, StoreSource],
                     storeConsumers: Map[StoreBuilder, Set[ToolBuilder]],
                     toolInputs: Map[ToolBuilder, Set[StoreBuilder]],
                     toolOutputs: Map[ToolBuilder, Set[StoreBuilder]]) {

  def +(store: StoreBuilder): LoamGraph = copy(stores = stores + store)

  def +(tool: ToolBuilder): LoamGraph =
    if (!tools(tool)) {
      val toolStores = tool.stores.toSet
      val toolInputStores = toolStores.filter(storeSources.contains)
      val toolOutputStores = toolStores -- toolInputStores
      val sourceFromTool = StoreSource.FromTool(tool)
      val outputsWithSource = toolOutputStores.map(store => store -> sourceFromTool)
      val inputsWithConsumers = toolInputStores.map({ store =>
        val consumers = storeConsumers.getOrElse(store, Set.empty) + tool
        store -> consumers
      }).toMap
      copy(tools = tools + tool, storeSources = storeSources ++ outputsWithSource,
        storeConsumers = storeConsumers ++ inputsWithConsumers, toolInputs = toolInputs + (tool -> toolInputStores),
        toolOutputs = toolOutputs + (tool -> toolOutputStores))
    } else {
      this
    }

  def +(store: StoreBuilder, source: StoreSource): LoamGraph = copy(storeSources = storeSources + (store -> source))

}
