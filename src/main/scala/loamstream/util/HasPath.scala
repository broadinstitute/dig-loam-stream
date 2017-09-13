package loamstream.util

import java.net.URI
import java.nio.file.{Path, Paths}

/**
 * @author kyuksel
 *         date: 8/30/17
 */
trait HasPath[P] {
  def path(p: P): Path
}

object HasPath {
  implicit object PathsArePaths extends HasPath[Path] {
    override def path(p: Path): Path = p
  }

  implicit object URIsHavePaths extends HasPath[URI] {
    override def path(u: URI): Path = Paths.get(u.getPath)
  }

  def base[A: HasPath](a: A): String = implicitly[HasPath[A]].path(a).getFileName.toString
}
