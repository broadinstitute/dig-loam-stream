package loamstream.model

import java.nio.file.Path

import loamstream.loam.{LoamGraph, LoamProjectContext, LoamScriptContext}
import loamstream.model.Tool.DefaultStores
import loamstream.util.Traversables

/**
  * @author Clint
  * @author Oliver
  *         date: Apr 26, 2016
  */
trait Tool extends LId.HasId {

  def name: String
  
  /** The LoamProjectContext associated with this tool */
  def projectContext: LoamProjectContext = scriptContext.projectContext

  /** The LoamScriptContext associated with this tool */
  def scriptContext: LoamScriptContext

  /** The graph this tool is part of */
  def graph: LoamGraph = projectContext.graph

  /** Input and output stores before any are specified using in or out */
  def defaultStores: DefaultStores

  def workDirOpt: Option[Path] = graph.workDirs.get(this)
  
  import Traversables.Implicits._
  
  /** Input stores of this tool */
  def inputs: Map[LId, Store] = {
    val myInputs: Set[Store] = graph.toolInputs.getOrElse(this, Set.empty)
    
    myInputs.mapBy(_.id)
  }

  /** Output stores of this tool */
  def outputs: Map[LId, Store] = {
    val myOutputs: Set[Store] = graph.toolOutputs.getOrElse(this, Set.empty)
    
    myOutputs.mapBy(_.id)
  }
}

object Tool {

  sealed trait DefaultStores {
    def all: Set[Store]
  }

  object DefaultStores {
    val empty: AllStores = AllStores(Set.empty)
  }

  final case class AllStores(stores: Set[Store]) extends DefaultStores {
    def all: Set[Store] = stores
  }

  final case class In(stores: Iterable[Store])

  object In {
    def apply(store: Store, stores: Store*): In = In(store +: stores)

    val empty: In = In(Set.empty)
  }

  final case class Out(stores: Iterable[Store])

  object Out {
    def apply(store: Store, stores: Store*): Out = Out(store +: stores)

    val empty: Out = Out(Set.empty)
  }

  final case class InputsAndOutputs(inputs: Iterable[Store],
                                    outputs: Iterable[Store]) extends DefaultStores {
    override def all: Set[Store] = (inputs ++ outputs).toSet
  }

  object InputsAndOutputs {
    def apply(in: In, out: Out): InputsAndOutputs = InputsAndOutputs(in.stores, out.stores)

    def apply(in: In): InputsAndOutputs = apply(in, Out.empty)

    def apply(out: Out): InputsAndOutputs = apply(In.empty, out)
  }

}
