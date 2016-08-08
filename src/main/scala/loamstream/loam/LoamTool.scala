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

  /** Input and output stores before any are specified using in or out */
  def defaultStores: Set[LoamStore]

  /** Input stores of this tool */
  override def inputs: Map[LId, Store] =
  graph.toolInputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  /** Output stores of this tool */
  override def outputs: Map[LId, Store] =
  graph.toolOutputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  /** Adds input stores to this tool */
  def in(inStore: LoamStore, inStores: LoamStore*): this.type = {
    graphBox(_.withInputStores(this, (inStore +: inStores).toSet))
    this
  }

  /** Adds output stores to this tool */
  def out(outStore: LoamStore, outStores: LoamStore*): this.type = {
    graphBox(_.withOutputStores(this, (outStore +: outStores).toSet))
    this
  }
}
