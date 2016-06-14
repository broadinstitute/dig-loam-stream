package loamstream.loam

import loamstream.loam.LoamGraph.StoreEdge

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
class LoamGraphBuilder {

  var graph = LoamGraph.empty

  def add(store: StoreBuilder): LoamGraphBuilder = {
    graph += store
    this
  }

  def add(tool: ToolBuilder): LoamGraphBuilder = {
    graph += tool
    this
  }

  def addSource(store: StoreBuilder, source: StoreEdge): LoamGraphBuilder = {
    graph = graph.withStoreSource(store, source)
    this
  }

  def addSink(store: StoreBuilder, sink: StoreEdge): LoamGraphBuilder = {
    graph = graph.withStoreSink(store, sink)
    this
  }

}
