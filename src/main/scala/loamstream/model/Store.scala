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

/**
 * @author oliverr
 * @author clint
 * Jun 8, 2016
 */
final class Store private (
    val id: LId, 
    val location: StoreLocation)(implicit val scriptContext: LoamScriptContext) extends HasLocation with LId.HasId {
    
  update() 
  
  override def equals(other: Any): Boolean = {
    other match {
      case that: Store => this.id == that.id
      case _ => false
    }
  }
  
  override def hashCode: Int = id.hashCode
  
  override def toString: String = s"store($id)@$render"
  
  def copy(id: LId = this.id, location: StoreLocation = this.location): Store = new Store(id, location)
    
  private def update(): Unit = projectContext.updateGraph(_.withStore(this))
    
  def projectContext: LoamProjectContext = scriptContext.projectContext

  def asInput: Store = {
    projectContext.updateGraph(_.withStoreAsInput(this))

    this
  }

  def graph: LoamGraph = projectContext.graph

  override def pathOpt: Option[Path] = location match {
    case StoreLocation.PathLocation(p) => Option(p)
    case _ => None
  }

  override def uriOpt: Option[URI] = location match  {
    case StoreLocation.UriLocation(u) => Option(u)
    case _ => None
  }
  
  override def path: Path = pathOpt.get
    
  override def uri: URI = uriOpt.get
  
  def render: String = {
    import BashScript.Implicits._
    
    location match {
      case StoreLocation.PathLocation(p) => p.render
      // if it's a URI then we can't shouldn't replace the path separator
      case StoreLocation.UriLocation(u) => u.toString
    }
  }

  def +(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixAdder(suffix))

  def -(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixRemover(suffix))
}

object Store {
  def create(implicit scriptContext: LoamScriptContext): Store = apply()
  
  def apply()(implicit scriptContext: LoamScriptContext): Store = apply(StoreLocation.PathLocation(anonPath))
  
  def apply(location: StoreLocation)(implicit scriptContext: LoamScriptContext): Store = {
    new Store(LId.newAnonId, location)
  }
  
  private def anonPath(implicit scriptContext: LoamScriptContext): Path = {
    Files.createTempFile(scriptContext.config.executionConfig.anonStoreDir, "loam", ".txt")
  }
}
