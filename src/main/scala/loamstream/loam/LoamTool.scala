package loamstream.loam

import loamstream.model.{LId, Store, Tool}
import loamstream.util.ValueBox

/** A tool defined in a Loam script */
trait LoamTool extends Tool {

  /** The unique tool id */
  def id: LId

  /** The ValueBox used to store the graph this tool is part of */
  def graphBox: ValueBox[LoamGraph]

  /** The graph this tool is part of */
  def graph: LoamGraph = graphBox.value

  /** Input stores of this tool */
  override def inputs: Map[LId, Store] =
  graph.toolInputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  /** Output stores of this tool */
  override def outputs: Map[LId, Store] =
  graph.toolOutputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  /** Adds input stores to this tool */
  def doIn(inStore: LoamStore, inStores: Seq[LoamStore]): Unit =
    graphBox(_.withInputStores(this, (inStore +: inStores).toSet))

  /** Adds output stores to this tool */
  def doOut(outStore: LoamStore, outStores: Seq[LoamStore]): Unit =
    graphBox(_.withOutputStores(this, (outStore +: outStores).toSet))

  /** Returns this after adding input stores to this tool */
  def in(inStore: LoamStore, inStores: LoamStore*): LoamTool = {
    doIn(inStore, inStores)
    this
  }

  /** Returns this after adding output stores to this tool */
  def out(outStore: LoamStore, outStores: LoamStore*): LoamTool = {
    doOut(outStore, outStores)
    this
  }
}
