package loamstream.loam

import loamstream.LEnv
import loamstream.loam.LoamGraph.StoreEdge

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
final class LoamGraphBuilder {

  @volatile private[this] var _graph: LoamGraph = LoamGraph.empty

  private[this] val lock = new AnyRef

  def graph: LoamGraph = lock.synchronized(_graph)

  def graph_=(newGraph: LoamGraph): Unit = _graph = newGraph

  private def updateGraph(f: LoamGraph => LoamGraph): Unit = lock.synchronized {
    graph = f(graph)
  }

  def addStore(store: LoamStore): LoamStore = {
    updateGraph(_.withStore(store))

    store
  }

  def addTool(tool: LoamTool, tokens: Seq[LoamToken]): LoamTool = {
    updateGraph(_.withTool(tool, tokens))

    tool
  }

  def addSource(store: LoamStore, source: StoreEdge): StoreEdge = {
    updateGraph(_.withStoreSource(store, source))

    source
  }

  def addSink(store: LoamStore, sink: StoreEdge): StoreEdge = {
    updateGraph(_.withStoreSink(store, sink))

    sink
  }

  def applyEnv(env: LEnv): Unit = updateGraph(_.withEnv(env))

  /** Adds input stores to this tool */
  def addInputStores(tool: LoamTool, stores: Set[LoamStore]): Unit = updateGraph(_.withInputStores(tool, stores))

  /** Adds output stores to this tool */
  def addOutputStores(tool: LoamTool, stores: Set[LoamStore]): Unit = updateGraph(_.withOutputStores(tool, stores))

  /** Sets two key slots to be the same set of keys (order may be different) */
  def setKeysSameSets(slot1: LoamStoreKeySlot, slot2: LoamStoreKeySlot): Unit =
  updateGraph(_.withKeysSameSet(slot1, slot2))

  /** Sets two key slots to be the same list of keys (same order) */
  def setKeysSameLists(slot1: LoamStoreKeySlot, slot2: LoamStoreKeySlot): Unit =
  updateGraph(_.withKeysSameList(slot1, slot2))
}
