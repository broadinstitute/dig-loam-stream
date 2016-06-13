package loamstream.util

import java.nio.file.Path
import scala.util.Try

/**
  * @author clint
  *         date: Jun 1, 2016
  */
object PathEnrichments {

  final implicit class PathHelpers(val path: Path) extends AnyVal {
    def /(next: String): Path = path.resolve(next)
    
    def /(next: Try[String]): Try[Path] = next.map(/)
  }
  
  final implicit class PathAttemptHelpers(val attempt: Try[Path]) extends AnyVal {
    def /(next: String): Try[Path] = attempt.map(_ / next)
    
    def /(next: Try[String]): Try[Path] = attempt.flatMap(_ / next)
  }

}