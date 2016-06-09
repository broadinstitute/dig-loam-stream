package loamstream.dsl

import java.nio.file.Path

import loamstream.LEnv
import loamstream.dsl.FlowBuilder.StoreSource
import loamstream.model.LId

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object FlowBuilder {

  trait StoreSource

  object StoreSource {

    case class FromPath(path: Path) extends StoreSource

    case class FromPathKey(key: LEnv.Key[Path]) extends StoreSource

    case class FromTool(tool: ToolBuilder) extends StoreSource

  }

}

class FlowBuilder {

  var stores: Set[StoreBuilder] = Set.empty
  var tools: Set[ToolBuilder] = Set.empty
  var storeSources: Map[StoreBuilder, StoreSource] = Map.empty
  var storeConsumers: Map[StoreBuilder, Set[ToolBuilder]] = Map.empty
  var toolInputs: Map[ToolBuilder, Set[StoreBuilder]] = Map.empty
  var toolOutputs: Map[ToolBuilder, Set[StoreBuilder]] = Map.empty

  def add(store: StoreBuilder): FlowBuilder = {
    stores += store
    this
  }

  def add(tool: ToolBuilder): FlowBuilder = {
    if (!tools(tool)) {
      tools += tool
      val toolStores = tool.stores.toSet
      val toolInputStores = toolStores.filter(storeSources.contains(_))
      val toolOutputStores = toolStores -- toolInputStores
      toolOutputStores.foreach(store => storeSources += store -> StoreSource.FromTool(tool))
      toolInputStores.foreach({ store =>
        val consumers = storeConsumers.getOrElse(store, Set.empty) + tool
        storeConsumers += (store -> consumers)
      })
      toolInputs += (tool -> toolInputStores)
      toolOutputs += (tool -> toolOutputStores)
    }
    this
  }

  def addSource(store: StoreBuilder, source: StoreSource): FlowBuilder = {
    storeSources += (store -> source)
    this
  }

  override def toString: String = {
    val storesString = stores.mkString("\n")
    val toolsString = tools.mkString("\n")
    val storeSourcesString = storeSources.values.mkString("\n")
    Seq(storesString, toolsString, storeSourcesString).mkString("\n")
  }

}
