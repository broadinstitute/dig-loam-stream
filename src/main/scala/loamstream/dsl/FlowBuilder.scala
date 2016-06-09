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

    case class FromTool(id: LId) extends StoreSource

  }
}

class FlowBuilder {

  var stores: Map[LId, StoreBuilder] = Map.empty
  var tools: Map[LId, ToolBuilder] = Map.empty
  var storeSources: Map[LId, StoreSource] = Map.empty

  def add(store: StoreBuilder): FlowBuilder = {
    stores += (store.id -> store)
    this
  }

  def add(tool: ToolBuilder): FlowBuilder = {
    tools += (tool.id -> tool)
    this
  }

  def addSource(store: StoreBuilder, source: StoreSource): FlowBuilder = {
    storeSources += (store.id -> source)
    this
  }

  override def toString: String = {
    val storesString = stores.values.mkString("\n")
    val toolsString = tools.values.mkString("\n")
    val storeSourcesString = storeSources.values.mkString("\n")
    Seq(storesString, toolsString, storeSourcesString).mkString("\n")
  }

}
