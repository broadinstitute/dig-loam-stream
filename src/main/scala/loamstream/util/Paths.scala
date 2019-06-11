package loamstream.util

import java.nio.file.Path
import java.nio.file.{Paths => JPaths}

import scala.util.Try
import java.time.Instant

/**
  * @author clint
  *         date: Jun 1, 2016
  */
object Paths {
  object Implicits {
    final implicit class PathHelpers(val path: Path) extends AnyVal {
      def /(next: String): Path = path.resolve(next)
      def /(next: Try[String]): Try[Path] = next.map(/)
      
      //scalastyle:off spaces.before.plus
      def +(next: String): Path = JPaths.get(s"${path.toString}$next")
      def +(next: Try[String]): Try[Path] = next.map(this.+)
      //scalastyle:on spaces.before.plus
    }
  
    final implicit class PathAttemptHelpers(val attempt: Try[Path]) extends AnyVal {
      def /(next: String): Try[Path] = attempt.map(_ / next)
      def /(next: Try[String]): Try[Path] = attempt.flatMap(_ / next)
      
      def +(next: String): Try[Path] = attempt.map(_ + next)
      def +(next: Try[String]): Try[Path] = attempt.flatMap(_ + next)
    }
  }
  
  def lastModifiedTime(p: Path): Instant = {
    val file = p.toFile

    val lastModifiedTimeInMillis = if (file.exists) file.lastModified else 0L

    Instant.ofEpochMilli(lastModifiedTimeInMillis)
  }

  // On Windows multiple drives require us to use the current working directory.
  def getRoot: Path = JPaths.get("/").toAbsolutePath

  def getCurrentDirectory: Path = JPaths.get(new java.io.File(".").getCanonicalPath)

  def newRelative(first: String, more: String*): Path = JPaths.get(first, more.toArray: _*)

  def newAbsolute(first: String, more: String*): Path = getRoot.resolve(newRelative(first, more: _*))

  def normalizePath(p: Path): Path = p.toAbsolutePath.normalize

  import BashScript.Implicits._
  
  def normalize(p: Path): String = normalizePath(p).render
  
  //NB: Basically anything path-separator-related
  private[this] val specialChars: Set[Char] = Set('/', ':', '\\', '$')
  
  def mungePathRelatedChars(s: String): String = s.map {
    case ch if ch.isWhitespace || specialChars.contains(ch) => '_'
    case ch => ch      
  }
}
