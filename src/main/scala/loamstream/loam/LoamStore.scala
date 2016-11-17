package loamstream.loam

import java.net.URI
import java.nio.file.{Path, Paths}

import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.ops.filters.{LoamStoreFilter, LoamStoreFilterTool, StoreFieldValueFilter}
import loamstream.loam.ops.{StoreField, StoreType}
import loamstream.model.{LId, Store}
import loamstream.util.{TypeBox, ValueBox}

import scala.reflect.runtime.universe.{Type, TypeTag}

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

  def create[Store <: StoreType : TypeTag](implicit scriptContext: LoamScriptContext): LoamStore[Store] =
    LoamStore[Store](LId.newAnonId, TypeBox.of[Store])

  def createOfType[Store <: StoreType](tpe: Type)(implicit scriptContext: LoamScriptContext): LoamStore[Store] =
    LoamStore[Store](LId.newAnonId, new TypeBox(tpe))
}

final case class LoamStore[Store <: StoreType] private(id: LId, sig: TypeBox[Store])(
  implicit val scriptContext: LoamScriptContext)
  extends LoamStore.Untyped {
  update()

  def from(path: String): LoamStore[Store] = from(Paths.get(path))

  def from(path: Path): LoamStore[Store] = from(StoreEdge.PathEdge(scriptContext.workDir.resolve(path)))

  def from(uri: URI): LoamStore[Store] = from(StoreEdge.UriEdge(uri))

  def from(source: StoreEdge): LoamStore[Store] = {
    graphBox.mutate(_.withStoreSource(this, source))
    this
  }

  def to(path: String): LoamStore[Store] = to(Paths.get(path))

  def to(path: Path): LoamStore[Store] = to(StoreEdge.PathEdge(scriptContext.workDir.resolve(path)))

  def to(uri: URI): LoamStore[Store] = to(StoreEdge.UriEdge(uri))

  def to(sink: StoreEdge): LoamStore[Store] = {
    graphBox.mutate(_.withStoreSink(this, sink))
    this
  }

  def filter[Value](field: StoreField[Store, Value])(valueFilter: Value => Boolean): LoamStore[Store] =
    filter(StoreFieldValueFilter(field, valueFilter))

  def filter(filter: LoamStoreFilter[Store]): LoamStore[Store] = {
    val outStore = filter.newOutStore(this)
    LoamStoreFilterTool(filter, this, outStore)
    outStore
  }

}



