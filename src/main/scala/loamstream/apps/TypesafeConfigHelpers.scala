package loamstream.apps

import loamstream.util.ConfigUtils
import java.nio.file.Path
import com.typesafe.config.Config

/**
 * @author clint
 * Oct 19, 2016
 */
trait TypesafeConfigHelpers {
  def configFromFile(confFile: Path): Config = ConfigUtils.configFromFile(confFile)
}
