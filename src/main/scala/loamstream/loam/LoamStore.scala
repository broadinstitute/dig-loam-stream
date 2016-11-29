package loamstream.loam

import java.net.URI
import java.nio.file.{Path, Paths}

import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.ops.StoreType.TXT
import loamstream.loam.ops.filters.{LoamStoreFilter, LoamStoreFilterTool, StoreFieldValueFilter}
import loamstream.loam.ops.mappers.{LoamStoreMapper, LoamStoreMapperTool, TextStoreFieldExtractor}
import loamstream.loam.ops.{StoreField, StoreType, TextStore, TextStoreField}
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

  def create[S <: StoreType : TypeTag](implicit scriptContext: LoamScriptContext): LoamStore[S] = {
    LoamStore[S](LId.newAnonId)
  }
}

final case class LoamStore[S <: StoreType : TypeTag] private(id: LId)(implicit val scriptContext: LoamScriptContext)
  extends LoamStore.Untyped {

  val sig: TypeBox[S] = TypeBox.of[S]

  update()

  def from(path: String): LoamStore[S] = from(Paths.get(path))

  def from(path: Path): LoamStore[S] = from(StoreEdge.PathEdge(scriptContext.workDir.resolve(path)))

  def from(uri: URI): LoamStore[S] = from(StoreEdge.UriEdge(uri))

  def from(source: StoreEdge): LoamStore[S] = {
    graphBox.mutate(_.withStoreSource(this, source))
    this
  }

  def to(path: String): LoamStore[S] = to(Paths.get(path))

  def to(path: Path): LoamStore[S] = to(StoreEdge.PathEdge(scriptContext.workDir.resolve(path)))

  def to(uri: URI): LoamStore[S] = to(StoreEdge.UriEdge(uri))

  def to(sink: StoreEdge): LoamStore[S] = {
    graphBox.mutate(_.withStoreSink(this, sink))
    this
  }

  def filter[V](field: StoreField[S, V])(valueFilter: V => Boolean): LoamStore[S] = {
    filter(StoreFieldValueFilter(field, valueFilter))
  }

  def filter(filter: LoamStoreFilter[S]): LoamStore[S] = {
    val outStore = filter.newOutStore
    LoamStoreFilterTool(filter, this, outStore)
    outStore
  }

  def map[SO <: StoreType : TypeTag](mapper: LoamStoreMapper[S, SO]): LoamStore[SO] = {
    val outStore = mapper.newOutStore
    LoamStoreMapperTool(mapper, this, outStore)
    outStore
  }

  def extract[V](field: TextStoreField[S with TextStore, V], defaultString: String): LoamStore[TXT] = {
    val mapper = TextStoreFieldExtractor[S with TextStore, V](field, defaultString)
    val outStore = mapper.newOutStore
    LoamStoreMapperTool(mapper, this.asInstanceOf[LoamStore[S with TextStore]], outStore)
    outStore
  }

}



