package loamstream.loam

import java.nio.file.{Path, Paths}

import loamstream.LEnv
import loamstream.loam.LoamGraph.StoreEdge
import loamstream.model.{LId, Store, StoreSig}

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object LoamStore {
  def create[T: TypeTag](implicit graphBuilder: LoamGraphBuilder): LoamStore =
    LoamStore(LId.newAnonId, StoreSig.create[T])
}

case class LoamStore private(id: LId, sig: StoreSig)(implicit graphBuilder: LoamGraphBuilder) extends Store {
  update()

  def update(): Unit = graphBuilder.addStore(this)

  def from(path: String): LoamStore = from(Paths.get(path))

  def from(path: Path): LoamStore = from(StoreEdge.PathEdge(path))

  def from(key: LEnv.Key[Path]): LoamStore = from(StoreEdge.PathKeyEdge(key))

  def from(source: StoreEdge): LoamStore = {
    graphBuilder.addSource(this, source)
    this
  }

  def to(path: String): LoamStore = to(Paths.get(path))

  def to(path: Path): LoamStore = to(StoreEdge.PathEdge(path))

  def to(key: LEnv.Key[Path]): LoamStore = to(StoreEdge.PathKeyEdge(key))

  def to(sink: StoreEdge): LoamStore = {
    graphBuilder.addSink(this, sink)
    this
  }

  override def toString: String = s"store[${sig.tpe}]"

  def graph: LoamGraph = graphBuilder.graph

  def pathOpt: Option[Path] = graph.pathOpt(this)

  def +(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixAdder(suffix))

  def -(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixRemover(suffix))
}



