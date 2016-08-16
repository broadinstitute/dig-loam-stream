package loamstream.loam

import loamstream.loam.LoamTool.DefaultStores
import loamstream.model.{LId, Store, Tool}
import loamstream.util.ValueBox

/** A tool defined in a Loam script */
trait LoamTool extends Tool {

  /** The unique tool id */
  def id: LId

  /** The LoamContext associated with this tool */
  def context: LoamContext

  /** The ValueBox used to store the graph this tool is part of */
  def graphBox: ValueBox[LoamGraph] = context.graphBox

  /** The graph this tool is part of */
  def graph: LoamGraph = graphBox.value

  /** Input and output stores before any are specified using in or out */
  def defaultStores: DefaultStores

  /** Input stores of this tool */
  override def inputs: Map[LId, Store] =
  graph.toolInputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  /** Output stores of this tool */
  override def outputs: Map[LId, Store] =
  graph.toolOutputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  /** Adds input stores to this tool */
  def in(inStore: LoamStore, inStores: LoamStore*): this.type = in(inStore +: inStores)

  /** Adds input stores to this tool */
  def in(inStores: Iterable[LoamStore]): this.type = {
    graphBox(_.withInputStores(this, inStores.toSet))
    this
  }

  /** Adds output stores to this tool */
  def out(outStore: LoamStore, outStores: LoamStore*): this.type = out(outStore +: outStores)

  /** Adds output stores to this tool */
  def out(outStores: Iterable[LoamStore]): this.type = {
    graphBox(_.withOutputStores(this, outStores.toSet))
    this
  }
}

object LoamTool {

  sealed trait DefaultStores {
    def all: Set[LoamStore]
  }

  object DefaultStores {
    val empty: AllStores = AllStores(Set.empty)
  }

  final case class AllStores(stores: Set[LoamStore]) extends DefaultStores {
    def all: Set[LoamStore] = stores
  }

  final case class In(stores: Iterable[LoamStore])

  object In {
    val empty: In = In(Set.empty)
  }

  final case class Out(stores: Iterable[LoamStore])

  object Out {
    val empty: Out = Out(Set.empty)
  }

  final case class InputsAndOutputs(inputs: Iterable[LoamStore], outputs: Iterable[LoamStore]) extends DefaultStores {
    override def all: Set[LoamStore] = (inputs ++ outputs).toSet
  }

  object InputsAndOutputs {
    def apply(in: In, out: Out): InputsAndOutputs = InputsAndOutputs(in.stores, out.stores)

    def apply(in: In): InputsAndOutputs = apply(in, Out.empty)

    def apply(out: Out): InputsAndOutputs = apply(In.empty, out)
  }

}
