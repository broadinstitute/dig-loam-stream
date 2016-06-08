package loamstream.dsl

import loamstream.model.LId

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
class FlowBuilder {

  var stores: Map[LId, StoreBuilder] = Map.empty
  var tools: Map[LId, ToolBuilder] = Map.empty

  def add(store: StoreBuilder): FlowBuilder = {
    stores += (store.id -> store)
    this
  }

  def add(tool: ToolBuilder): FlowBuilder = {
    tools += (tool.id -> tool)
    this
  }

}
