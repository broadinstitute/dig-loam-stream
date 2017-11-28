package loamstream.conf

import scala.util.Try
import java.nio.file.Path
import com.typesafe.config.Config
import loamstream.util.ConfigUtils

/**
 * @author clint
 * Nov 2, 2017
 */
trait ConfigParser[A] {

  def fromConfig(config: Config): Try[A]
  
  final def fromPath(configFilePath: Path): Try[A] = fromConfig(ConfigUtils.configFromFile(configFilePath))
}
