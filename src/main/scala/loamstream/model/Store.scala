package loamstream.model

import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths

import loamstream.loam.HasLocation
import loamstream.loam.LoamGraph
import loamstream.loam.LoamGraph.StoreLocation
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamStoreRef
import java.nio.file.Files
import loamstream.util.BashScript
import loamstream.model.LId.LAnonId

/**
 * @author oliverr
 * @author clint
 * Jun 8, 2016
 */
sealed abstract class Store protected (val id: LId) extends HasLocation with LId.HasId {
    
  implicit val scriptContext: LoamScriptContext
  
  override def equals(other: Any): Boolean = {
    other match {
      case that: Store => this.id == that.id
      case _ => false
    }
  }
  
  override def hashCode: Int = id.hashCode
  
  override def toString: String = s"store($id)@$render"
  
  def projectContext: LoamProjectContext = scriptContext.projectContext

  def asInput: Store = {
    projectContext.updateGraph(_.withStoreAsInput(this))

    this
  }
  
  def isInput: Boolean = graph.inputStores.contains(this)

  def graph: LoamGraph = projectContext.graph
  
  def +(suffix: String): LoamStoreRef = {
    if(pathOpt.isDefined) {
      LoamStoreRef(this, pathModifier = LoamStoreRef.pathSuffixAdder(suffix))
    } else {
      LoamStoreRef(this, uriModifier = LoamStoreRef.uriSuffixAdder(suffix))
    }
  }

  def -(suffix: String): LoamStoreRef = {
    if(pathOpt.isDefined) {
      LoamStoreRef(this, pathModifier = LoamStoreRef.pathSuffixRemover(suffix))
    } else {
      LoamStoreRef(this, uriModifier = LoamStoreRef.uriSuffixRemover(suffix))
    }
  }
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
