package loamstream.conf

import scala.util.Try

import com.typesafe.config.Config

/**
 * @author clint
 * date: Jun 13, 2016
 */
trait ConfigCompanion[C] {
  def fromConfig(config: Config): Try[C]
    
  def fromFile(configFile: String): Try[C] = fromConfig(TypesafeConfig.fromFile(configFile))
}