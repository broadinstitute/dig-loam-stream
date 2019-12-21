package loamstream.model

import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths

import loamstream.loam.LoamGraph
import loamstream.loam.LoamGraph.StoreLocation
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScriptContext
import java.nio.file.Files
import loamstream.util.BashScript
import loamstream.model.LId.LAnonId

/**
 * @author oliverr
 * @author clint
 * Jun 8, 2016
 */
sealed abstract class Store protected (val id: LId) extends LId.HasId {
    
  implicit val scriptContext: LoamScriptContext
  
  def path: Path 
  
  def pathOpt: Option[Path]
  
  def uri: URI
  
  def uriOpt: Option[URI]
  
  def render: String
  
  override def equals(other: Any): Boolean = {
    other match {
      case that: Store => this.id == that.id
      case _ => false
    }
  }
  
  override def hashCode: Int = id.hashCode
  
  final override def toString: String = s"store($id)@$render"
  
  final def projectContext: LoamProjectContext = scriptContext.projectContext

  final def asInput: Store = {
    projectContext.updateGraph(_.withStoreAsInput(this))

    this
  }
  
  final def isInput: Boolean = graph.inputStores.contains(this)

  final def graph: LoamGraph = projectContext.graph
  
  final def isPathStore: Boolean = this.isInstanceOf[PathStore]
  
  final def isUriStore: Boolean = this.isInstanceOf[UriStore]
}

final case class PathStore(
    override val id: LId, 
    path: Path)(implicit override val scriptContext: LoamScriptContext) extends Store(id) {
  
  override def pathOpt: Option[Path] = Option(path)

  override def uriOpt: Option[URI] = None
    
  override def uri: URI = uriOpt.get
  
  override def render: String = Store.render(path)
}

final case class UriStore(
    override val id: LId, 
    uri: URI)(implicit override val scriptContext: LoamScriptContext) extends Store(id) {
  
  override def pathOpt: Option[Path] = None
  
  override def uriOpt: Option[URI] = Option(uri)
  
  override def path: Path = pathOpt.get
  
  override def render: String = Store.render(uri)
}

object Store {
  private[model] def apply(id: LId, location: StoreLocation)(implicit scriptContext: LoamScriptContext): Store = {
    val store = location match {
      case StoreLocation.PathLocation(p) => PathStore(id, p)
      case StoreLocation.UriLocation(u) => UriStore(id, u)
    }
    
    scriptContext.projectContext.updateGraph(_.withStore(store))
    
    store
  }
  
  private[model] def apply(path: Path)(implicit scriptContext: LoamScriptContext): Store = {
    Store(StoreLocation.PathLocation(path))
  }
  
  private[model] def apply(uri: URI)(implicit scriptContext: LoamScriptContext): Store = {
    Store(StoreLocation.UriLocation(uri))
  }
  
  def apply(location: StoreLocation)(implicit scriptContext: LoamScriptContext): Store = apply(LId.newAnonId, location)
  
  def apply()(implicit scriptContext: LoamScriptContext): Store = apply(anonPath)
  
  private def anonPath(implicit scriptContext: LoamScriptContext): Path = {
    Files.createTempFile(scriptContext.config.executionConfig.anonStoreDir, "loam", ".txt")
  }
  
  def render(p: Path): String = {
    import BashScript.Implicits._
    
    p.render
  }
  
  def render(u: URI): String = u.toString
}
