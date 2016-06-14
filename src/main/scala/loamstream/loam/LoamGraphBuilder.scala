package loamstream.loam

import loamstream.loam.LoamGraph.StoreEdge

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
class LoamGraphBuilder {

  var graph = LoamGraph.empty

  def addStore(store: LoamStore): LoamStore = {
    graph = graph.withStore(store)
    store
  }

  def addTool(tool: LoamTool, tokens: Seq[LoamToken]): LoamTool = {
    graph = graph.withTool(tool, tokens)
    tool
  }

  def addSource(store: LoamStore, source: StoreEdge): StoreEdge = {
    graph = graph.withStoreSource(store, source)
    source
  }

  def addSink(store: LoamStore, sink: StoreEdge): StoreEdge = {
    graph = graph.withStoreSink(store, sink)
    sink
  }

}
