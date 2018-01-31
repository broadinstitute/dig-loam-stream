package loamstream.loam

import java.nio.file.Path

import loamstream.loam.files.LoamFileManager
import loamstream.model.Store
import loamstream.util.BashScript
import loamstream.util.PathUtils
import loamstream.util.TypeBox

/** A reference to a Loam store and a path modifier to be used in command line tools */
final case class LoamStoreRef(store: Store, pathModifier: Path => Path) extends HasLocation {
  import BashScript.Implicits._

  override def path: Path = path(store.projectContext.fileManager)

  /** The path to be used in command line tools */
  def path(fileManager: LoamFileManager): Path = pathModifier(fileManager.getPath(store))

  override def render(fileManager: LoamFileManager): String = path(fileManager).render
}

object LoamStoreRef {
  val pathIdentity: Path => Path = identity

  def suffixAdder(suffix: String): Path => Path = PathUtils.getFileNameTransformation(_ + suffix)

  def suffixRemover(suffix: String): Path => Path = PathUtils.getFileNameTransformation { fileName =>
    if (fileName.endsWith(suffix)) { fileName.dropRight(suffix.length) }
    else { fileName }
  }

}
