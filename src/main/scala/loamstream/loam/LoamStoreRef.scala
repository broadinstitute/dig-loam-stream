package loamstream.loam

import java.nio.file.Path

import loamstream.loam.files.LoamFileManager
import loamstream.util.PathUtil

/** A reference to a Loam store and a path modifier to be used in command line tools */
case class LoamStoreRef(store: LoamStore, pathModifier: Path => Path) {

  /** The path to be used in command line tools */
  def path(fileManager: LoamFileManager): Path = pathModifier(fileManager.getPath(store))
}

object LoamStoreRef {
  val pathIdentity: Path => Path = path => path

  def suffixAdder(suffix: String): Path => Path = PathUtil.getFileNameTransformation(_ + suffix)

  def suffixRemover(suffix: String): Path => Path = PathUtil.getFileNameTransformation({
    fileName =>
      if (fileName.endsWith(suffix)) {
        fileName.substring(0, fileName.length - suffix.length)
      } else {
        fileName
      }
  })

}