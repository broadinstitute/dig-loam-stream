package loamstream.loam

import java.nio.file.{Path, Paths}

import loamstream.loam.files.LoamFileManager

/** A reference to a Loam store and a path modifier to be used in command line tools */
case class LoamStoreRef(store: LoamStore, pathModifier: Path => Path) {

  /** The path to be used in command line tools */
  def path(fileManager: LoamFileManager): Path = pathModifier(fileManager.getPath(store))
}

object LoamStoreRef {
  val pathIdentity: Path => Path = path => path

  def suffixAdder(suffix: String): Path => Path = { path => Paths.get(path.toString + suffix) }

  def suffixRemover(suffix: String): Path => Path = { path =>
    val pathString = path.toString
    val pathStringPruned = if (pathString.endsWith(suffix)) {
      pathString.substring(0, pathString.length - suffix.length)
    } else {
      pathString
    }
    Paths.get(pathStringPruned)
  }


}