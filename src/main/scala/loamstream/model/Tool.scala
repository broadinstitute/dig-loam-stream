package loamstream.model

import java.nio.file.Path

import loamstream.loam.{LoamGraph, LoamProjectContext, LoamScriptContext}
import loamstream.model.Tool.DefaultStores

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

  /** Input stores of this tool */
  def inputs: Map[LId, Store] = {
    graph.toolInputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap
  }

  /** Output stores of this tool */
  def outputs: Map[LId, Store] = {
    graph.toolOutputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap
  }

  /** Adds input stores to this tool */
  /*def in(inStore: Store, inStores: Store*): this.type = in(inStore +: inStores)

  *//** Adds input stores to this tool *//*
  def in(inStores: Iterable[Store]): this.type = {
    projectContext.updateGraph(_.withInputStores(this, inStores.toSet))
    
    this
  }

  *//** Adds output stores to this tool *//*
  def out(outStore: Store, outStores: Store*): this.type = out(outStore +: outStores)

  *//** Adds output stores to this tool *//*
  def out(outStores: Iterable[Store]): this.type = {
    projectContext.updateGraph(_.withOutputStores(this, outStores.toSet))
    
    this
  }*/

  def workDirOpt: Option[Path] = graph.workDirs.get(this)
  
  /*@deprecated(message = "Use tag(name) instead", since = "")
  def named(name: String): this.type = tag(name)
  
  def tag(name: String): this.type = {
    projectContext.updateGraph(_.withToolName(this, name))
    
    this
  }*/
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
