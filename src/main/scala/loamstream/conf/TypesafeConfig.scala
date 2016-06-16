package loamstream.conf

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import java.nio.file.Path
import java.nio.file.Paths

/**
 * @author clint
 * date: Jun 1, 2016
 */
object TypesafeConfig {
  val DefaultConfigPrefix = "loamstream"
  
  lazy val defaultConfig = ConfigFactory.load(DefaultConfigPrefix)
  
  def fromFile(path: Path): Config = ConfigFactory.parseFile(path.toFile).withFallback(defaultConfig)
  
  def fromFile(fileName: String): Config = fromFile(Paths.get(fileName))
  
  abstract class KeyHolder(base: String) {
    def key(k: String): String = s"$base.$k"
  }
}