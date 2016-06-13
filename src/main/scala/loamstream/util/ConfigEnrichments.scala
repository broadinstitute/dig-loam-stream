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
    
    def tryGetPath(path: String): Try[Path] = tryGet(path)(Paths.get(_))
    
    def tryGetInt(path: String): Try[Int] = tryGet(path)(_.toInt)
    
    def tryGetConfig(path: String): Try[Config] = Try(config.getConfig(path))
    
    private def tryGet[A](path: String)(f: String => A): Try[A] = tryGetString(path).map(f)
  }
}