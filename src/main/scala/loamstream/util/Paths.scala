package loamstream.util

import java.nio.file.{Path, Paths => JPaths}

import scala.util.Try

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
}
