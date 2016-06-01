package loamstream.util

import com.typesafe.config.Config
import scala.util.Try
import java.nio.file.Path
import java.nio.file.Paths

/**
 * @author clint
 * date: Jun 1, 2016
 */
object ConfigEnrichments {
  final implicit class ConfigOps(val config: Config) extends AnyVal {
    def tryGetString(path: String): Try[String] = Try(config.getString(path))
    
    def tryGetPath(path: String): Try[Path] = tryGetString(path).map(Paths.get(_))
  }
}