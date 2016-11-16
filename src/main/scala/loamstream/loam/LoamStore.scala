package loamstream.loam

import java.net.URI
import java.nio.file.{Path, Paths}

import loamstream.loam.LoamGraph.StoreEdge
import loamstream.model.{LId, Store}
import loamstream.util.{TypeBox, ValueBox}

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object LoamStore {

  trait Untyped extends Store {
    def id: LId

    def sig: TypeBox.Untyped

    def scriptContext: LoamScriptContext

    def projectContext: LoamProjectContext = scriptContext.projectContext

    def graphBox: ValueBox[LoamGraph] = projectContext.graphBox

    def update(): Unit = graphBox.mutate(_.withStore(this))

    def from(path: String): LoamStore.Untyped

    def from(path: Path): LoamStore.Untyped

    def from(uri: URI): LoamStore.Untyped

    def from(source: StoreEdge): LoamStore.Untyped

    def to(path: String): LoamStore.Untyped

    def to(path: Path): LoamStore.Untyped

    def to(uri: URI): LoamStore.Untyped

    def to(sink: StoreEdge): LoamStore.Untyped

    def key(name: String): LoamStoreKeySlot = LoamStoreKeySlot(this, name)(projectContext)

    override def toString: String = s"store[${sig.tpe}]"

    def graph: LoamGraph = graphBox.value

    def pathOpt: Option[Path] = graph.pathOpt(this)

    def path: Path = projectContext.fileManager.getPath(this)

    def uriOpt: Option[URI] = graph.uriOpt(this)

    def +(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixAdder(suffix))

    def -(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixRemover(suffix))

  }

  def create[T: TypeTag](implicit scriptContext: LoamScriptContext): LoamStore[T] =
    LoamStore[T](LId.newAnonId, TypeBox.of[T])
}

final case class LoamStore[T] private(id: LId, sig: TypeBox[T])(implicit val scriptContext: LoamScriptContext)
  extends LoamStore.Untyped {
  update()

  def from(path: String): LoamStore[T] = from(Paths.get(path))

  def from(path: Path): LoamStore[T] = from(StoreEdge.PathEdge(scriptContext.workDir.resolve(path)))

  def from(uri: URI): LoamStore[T] = from(StoreEdge.UriEdge(uri))

  def from(source: StoreEdge): LoamStore[T] = {
    graphBox.mutate(_.withStoreSource(this, source))
    this
  }

  def to(path: String): LoamStore[T] = to(Paths.get(path))

  def to(path: Path): LoamStore[T] = to(StoreEdge.PathEdge(scriptContext.workDir.resolve(path)))

  def to(uri: URI): LoamStore[T] = to(StoreEdge.UriEdge(uri))

  def to(sink: StoreEdge): LoamStore[T] = {
    graphBox.mutate(_.withStoreSink(this, sink))
    this
  }

}



