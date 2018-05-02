package loamstream.loam

import java.nio.file.Path

import loamstream.model.Store
import loamstream.util.BashScript
import loamstream.util.PathUtils
import loamstream.util.TypeBox
import java.net.URI

/** A reference to a Loam store and a path modifier to be used in command line tools */
final case class LoamStoreRef(store: Store, pathModifier: Path => Path) extends HasLocation {
  import BashScript.Implicits._

  override def path: Path = pathModifier(store.path)

  override def uri: URI = ??? //TODO
  
  override def render: String = path.render
}

object LoamStoreRef {
  val pathIdentity: Path => Path = identity

  def suffixAdder(suffix: String): Path => Path = PathUtils.getFileNameTransformation(_ + suffix)

  def suffixRemover(suffix: String): Path => Path = PathUtils.getFileNameTransformation { fileName =>
    if (fileName.endsWith(suffix)) { fileName.dropRight(suffix.length) }
    else { fileName }
  }

}
