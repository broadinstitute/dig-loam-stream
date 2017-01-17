package loamstream.loam

import java.net.URI
import java.nio.file.{Path, Paths}

import loamstream.loam.LoamGraph.{StoreEdge, StoreLocation}
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

    def from(location: StoreLocation, source: StoreEdge): LoamStore.Untyped

    def to(path: String): LoamStore.Untyped

    def to(path: Path): LoamStore.Untyped

    def to(uri: URI): LoamStore.Untyped

    def to(location: StoreLocation, sink: StoreEdge): LoamStore.Untyped

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

  override def from(path: String): LoamStore[S] = from(Paths.get(path))

  override def from(path: Path): LoamStore[S] = {
    val resolvedPath = scriptContext.workDir.resolve(path)
    val location = StoreLocation.PathLocation(resolvedPath)
    val source = StoreEdge.PathEdge(resolvedPath)
    from(location, source)
  }

  override def from(uri: URI): LoamStore[S] = {
    val location = StoreLocation.UriLocation(uri)
    val source = StoreEdge.UriEdge(uri)
    from(location, source)
  }

  override def from(location: StoreLocation, source: StoreEdge): LoamStore[S] = {
    graphBox.mutate(_.withStoreLocation(this, location))
    graphBox.mutate(_.withStoreSource(this, source))
    this
  }

  override def to(path: String): LoamStore[S] = to(Paths.get(path))

  override def to(path: Path): LoamStore[S] = {
    val resolvedPath = scriptContext.workDir.resolve(path)
    val location = StoreLocation.PathLocation(resolvedPath)
    val sink = StoreEdge.PathEdge(resolvedPath)
    to(location, sink)
    }

  override def to(uri: URI): LoamStore[S] = {
    val location = StoreLocation.UriLocation(uri)
    val sink = StoreEdge.UriEdge(uri)
    to(location, sink)
  }

  override def to(location: StoreLocation, sink: StoreEdge): LoamStore[S] = {
    graphBox.mutate(_.withStoreLocation(this, location))
    graphBox.mutate(_.withStoreSink(this, sink))
    this
  }

  /** Returns new store which is the result of a new store filtering tool based on a field
    *
    * Store records are retained if given field has a value passing the valueFilter.
    */
  def filter[V](field: StoreField[S, V])(valueFilter: V => Boolean): LoamStore[S] = {
    filter(StoreFieldValueFilter(field, valueFilter))
  }

  /** Returns new store which is the result of a new store filtering tool based on given filter */
  def filter(filter: LoamStoreFilter[S]): LoamStore[S] = {
    val outStore = filter.newOutStore
    LoamStoreFilterTool(filter, this, outStore)
    outStore
  }

  /** Returns new store which is the result of a new store mapping tool based on given mapper */
  def map[SO <: StoreType : TypeTag](mapper: LoamStoreMapper[S, SO]): LoamStore[SO] = {
    val outStore = mapper.newOutStore
    LoamStoreMapperTool(mapper, this, outStore)
    outStore
  }

  /** Returns new store of type TXT based on mapping extracting given field */
  def extract[V](field: TextStoreField[S with TextStore, V],
                 defaultString: String = TextStoreFieldExtractor.defaultNA): LoamStore[TXT] = {
    val mapper = TextStoreFieldExtractor[S with TextStore, V](field, defaultString)
    val outStore = mapper.newOutStore
    LoamStoreMapperTool(mapper, this.asInstanceOf[LoamStore[S with TextStore]], outStore)
    outStore
  }

}



