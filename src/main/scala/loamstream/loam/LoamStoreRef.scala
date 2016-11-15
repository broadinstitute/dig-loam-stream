package loamstream.loam

import java.nio.file.Path

import loamstream.loam.files.LoamFileManager
import loamstream.util.PathUtils

/** A reference to a Loam store and a path modifier to be used in command line tools */
final case class LoamStoreRef(store: LoamStore.Untyped, pathModifier: Path => Path) {

  /** The path to be used in command line tools */
  def path(fileManager: LoamFileManager): Path = pathModifier(fileManager.getPath(store))
}

object LoamStoreRef {
  val pathIdentity: Path => Path = identity

  def suffixAdder(suffix: String): Path => Path = PathUtils.getFileNameTransformation(_ + suffix)

  def suffixRemover(suffix: String): Path => Path = PathUtils.getFileNameTransformation {
    fileName =>
      if (fileName.endsWith(suffix)) { fileName.dropRight(suffix.length) } 
      else { fileName }
  }

}