package loamstream.model

import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths

import loamstream.loam.HasLocation
import loamstream.loam.LoamGraph
import loamstream.loam.LoamGraph.StoreLocation
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamStoreKeySlot
import loamstream.loam.LoamStoreRef

/**
 * @author oliverr
 * @author clint
 * Jun 8, 2016
 */
trait Store extends HasLocation with LId.HasId {
  def scriptContext: LoamScriptContext

  def projectContext: LoamProjectContext = scriptContext.projectContext

  def update(): Unit = projectContext.updateGraph(_.withStore(this))

  def asInput: Store = {
    projectContext.updateGraph(_.withStoreAsInput(this))

    this
  }

  def at(path: String): Store = at(Paths.get(path))

  def at(path: Path): Store = {
    val resolvedPath = scriptContext.workDir.resolve(path)
    val location = StoreLocation.PathLocation(resolvedPath)
    at(location)
  }

  def at(uri: URI): Store = {
    val location = StoreLocation.UriLocation(uri)
    at(location)
  }

  def at(location: StoreLocation): Store = {
    projectContext.updateGraph(_.withStoreLocation(this, location))

    this
  }

  def key(name: String): LoamStoreKeySlot = LoamStoreKeySlot(this, name)(projectContext)

  override def toString: String = {
    val location = (pathOpt orElse uriOpt).map(_.toString).getOrElse(path)

    s"store($id)@$location"
  }

  def graph: LoamGraph = projectContext.graph

  override def pathOpt: Option[Path] = graph.pathOpt(this)

  override def path: Path = projectContext.fileManager.getPath(this)

  override def uriOpt: Option[URI] = graph.uriOpt(this)

  def +(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixAdder(suffix))

  def -(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixRemover(suffix))
}

object Store {
  def create(implicit scriptContext: LoamScriptContext): Store = DefaultStore(LId.newAnonId)

  private final case class DefaultStore private (id: LId)(implicit val scriptContext: LoamScriptContext)
      extends Store {

    update()

  }
}
