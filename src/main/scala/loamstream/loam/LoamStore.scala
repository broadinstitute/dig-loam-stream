package loamstream.loam

import java.nio.file.{Path, Paths}

import loamstream.loam.LoamGraph.StoreEdge
import loamstream.model.{LId, Store, StoreSig}
import loamstream.util.ValueBox

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object LoamStore {
  def create[T: TypeTag](implicit context: LoamContext): LoamStore = LoamStore(LId.newAnonId, StoreSig.create[T])
}

final case class LoamStore private(id: LId, sig: StoreSig)(implicit context: LoamContext) extends Store {
  update()

  def graphBox: ValueBox[LoamGraph] = context.graphBox

  def update(): Unit = graphBox(_.withStore(this))

  def from(path: String): LoamStore = from(Paths.get(path))

  def from(path: Path): LoamStore = from(StoreEdge.PathEdge(path))

  def from(source: StoreEdge): LoamStore = {
    graphBox(_.withStoreSource(this, source))
    this
  }

  def to(path: String): LoamStore = to(Paths.get(path))

  def to(path: Path): LoamStore = to(StoreEdge.PathEdge(path))

  def to(sink: StoreEdge): LoamStore = {
    graphBox(_.withStoreSink(this, sink))
    this
  }

  def key(name: String): LoamStoreKeySlot = LoamStoreKeySlot(this, name)

  override def toString: String = s"store[${sig.tpe}]"

  def graph: LoamGraph = graphBox.value

  def pathOpt: Option[Path] = graph.pathOpt(this)

  def path: Path = context.fileManager.getPath(this)

  def +(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixAdder(suffix))

  def -(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixRemover(suffix))
}



