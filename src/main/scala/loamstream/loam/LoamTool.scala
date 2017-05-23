package loamstream.loam

import java.nio.file.Path

import loamstream.loam.LoamTool.DefaultStores
import loamstream.model.{LId, Store, Tool}
import loamstream.util.ValueBox

/** A tool defined in a Loam script */
trait LoamTool extends Tool {

  /** The unique tool id */
  def id: LId

  /** The LoamProjectContext associated with this tool */
  def projectContext: LoamProjectContext = scriptContext.projectContext

  /** The LoamScriptContext associated with this tool */
  def scriptContext: LoamScriptContext

  /** The ValueBox used to store the graph this tool is part of */
  def graphBox: ValueBox[LoamGraph] = projectContext.graphBox

  /** The graph this tool is part of */
  def graph: LoamGraph = graphBox.value

  /** Input and output stores before any are specified using in or out */
  def defaultStores: DefaultStores

  /** Input stores of this tool */
  override def inputs: Map[LId, Store.Untyped] =
  graph.toolInputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  /** Output stores of this tool */
  override def outputs: Map[LId, Store.Untyped] =
  graph.toolOutputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  /** Adds input stores to this tool */
  def in(inStore: Store.Untyped, inStores: Store.Untyped*): this.type = in(inStore +: inStores)

  /** Adds input stores to this tool */
  def in(inStores: Iterable[Store.Untyped]): this.type = {
    graphBox.mutate(_.withInputStores(this, inStores.toSet))
    this
  }

  /** Adds output stores to this tool */
  def out(outStore: Store.Untyped, outStores: Store.Untyped*): this.type = out(outStore +: outStores)

  /** Adds output stores to this tool */
  def out(outStores: Iterable[Store.Untyped]): this.type = {
    graphBox.mutate(_.withOutputStores(this, outStores.toSet))
    this
  }

  def workDirOpt: Option[Path] = graphBox.get(_.workDirs.get(this))
}

object LoamTool {

  sealed trait DefaultStores {
    def all: Set[Store.Untyped]
  }

  object DefaultStores {
    val empty: AllStores = AllStores(Set.empty)
  }

  final case class AllStores(stores: Set[Store.Untyped]) extends DefaultStores {
    def all: Set[Store.Untyped] = stores
  }

  final case class In(stores: Iterable[Store.Untyped])

  object In {
    def apply(store: Store.Untyped, stores: Store.Untyped*): In = In(store +: stores)

    val empty: In = In(Set.empty)
  }

  final case class Out(stores: Iterable[Store.Untyped])

  object Out {
    def apply(store: Store.Untyped, stores: Store.Untyped*): Out = Out(store +: stores)

    val empty: Out = Out(Set.empty)
  }

  final case class InputsAndOutputs(inputs: Iterable[Store.Untyped],
                                    outputs: Iterable[Store.Untyped]) extends DefaultStores {
    override def all: Set[Store.Untyped] = (inputs ++ outputs).toSet
  }

  object InputsAndOutputs {
    def apply(in: In, out: Out): InputsAndOutputs = InputsAndOutputs(in.stores, out.stores)

    def apply(in: In): InputsAndOutputs = apply(in, Out.empty)

    def apply(out: Out): InputsAndOutputs = apply(In.empty, out)
  }

}
