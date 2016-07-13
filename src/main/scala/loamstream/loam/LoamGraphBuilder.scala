package loamstream.loam

import loamstream.LEnv
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

  def applyEnv(env: LEnv): Unit = {
    graph = graph.withEnv(env)
  }

  /** Adds input stores to this tool */
  def addInputStores(tool: LoamTool, stores: Set[LoamStore]): Unit = {
    graph = graph.withInputStores(tool, stores)
  }

  /** Adds output stores to this tool */
  def addOutputStores(tool: LoamTool, stores: Set[LoamStore]): Unit = {
    graph = graph.withInputStores(tool, stores)
  }
}
