package loamstream.loam

import java.nio.file.Path

import loamstream.model.Store
import loamstream.util.BashScript
import loamstream.util.PathUtils
import loamstream.util.TypeBox
import java.net.URI

/** A reference to a Loam store and a path modifier to be used in command line tools */
final case class LoamStoreRef(
    store: Store, 
    pathModifier: Path => Path = identity, 
    uriModifier: URI => URI = identity) extends HasLocation {
  
  import BashScript.Implicits._

  override def pathOpt: Option[Path] = store.pathOpt.map(pathModifier)
  
  override def path: Path = pathModifier(store.path)

  override def uriOpt: Option[URI] = store.uriOpt.map(uriModifier)
  
  override def uri: URI = uriModifier(store.uri)
  
  override def render: String = pathOpt.orElse(uriOpt) match {
    case Some(p: Path) => Store.render(p)
    case Some(u: URI) => Store.render(u)
    case _ => sys.error("Underlying Store must be backed by a Path or a URI")
  }
  
  //TODO: Add + and -, if those stay in Store
}

object LoamStoreRef {
  def pathSuffixAdder(suffix: String): Path => Path = PathUtils.getFileNameTransformation(_ + suffix)

  def pathSuffixRemover(suffix: String): Path => Path = PathUtils.getFileNameTransformation { fileName =>
    if (fileName.endsWith(suffix)) { fileName.dropRight(suffix.length) }
    else { fileName }
  }

  def uriSuffixAdder(suffix: String): URI => URI = { original =>
    URI.create(original.toString + suffix)
  }
  
  def uriSuffixRemover(suffix: String): URI => URI = { original =>
    val originalAsString = original.toString
    
    val resultAsString = {
      if (originalAsString.endsWith(suffix)) { originalAsString.dropRight(suffix.length) }
      else { originalAsString }
    }
    
    URI.create(resultAsString)
  }
}
