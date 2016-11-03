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

  trait Untyped extends Store {
    def id: LId

    def sig: StoreSig

    def scriptContext: LoamScriptContext

    def projectContext: LoamProjectContext = scriptContext.projectContext

    def graphBox: ValueBox[LoamGraph] = projectContext.graphBox

    def update(): Unit = graphBox.mutate(_.withStore(this))

    def from(path: String): LoamStore.Untyped

    def from(path: Path): LoamStore.Untyped

    def from(source: StoreEdge): LoamStore.Untyped

    def to(path: String): LoamStore.Untyped

    def to(path: Path): LoamStore.Untyped

    def to(sink: StoreEdge): LoamStore.Untyped

    def key(name: String): LoamStoreKeySlot = LoamStoreKeySlot(this, name)(projectContext)

    override def toString: String = s"store[${sig.tpe}]"

    def graph: LoamGraph = graphBox.value

    def pathOpt: Option[Path] = graph.pathOpt(this)

    def path: Path = projectContext.fileManager.getPath(this)

    def +(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixAdder(suffix))

    def -(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixRemover(suffix))

  }

  def create[T: TypeTag](implicit scriptContext: LoamScriptContext): LoamStore[T] =
    LoamStore[T](LId.newAnonId, StoreSig.create[T])
}

final case class LoamStore[T] private(id: LId, sig: StoreSig)(implicit val scriptContext: LoamScriptContext)
  extends LoamStore.Untyped {
  update()

  def from(path: String): LoamStore[T] = from(Paths.get(path))

  def from(path: Path): LoamStore[T] = from(StoreEdge.PathEdge(scriptContext.workDir.resolve(path)))

  def from(source: StoreEdge): LoamStore[T] = {
    graphBox.mutate(_.withStoreSource(this, source))
    this
  }

  def to(path: String): LoamStore[T] = to(Paths.get(path))

  def to(path: Path): LoamStore[T] = to(StoreEdge.PathEdge(scriptContext.workDir.resolve(path)))

  def to(sink: StoreEdge): LoamStore[T] = {
    graphBox.mutate(_.withStoreSink(this, sink))
    this
  }

}



