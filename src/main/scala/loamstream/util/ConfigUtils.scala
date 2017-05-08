package loamstream.util

import java.nio.file.Path
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * @author clint
 * Oct 19, 2016
 */
object ConfigUtils {
  def configFromFile(confFile: Path): Config = ConfigFactory.parseFile(confFile.toAbsolutePath.toFile)
}
