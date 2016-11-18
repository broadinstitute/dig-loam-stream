package loamstream.apps

import java.nio.file.Path

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * @author clint
 * Oct 19, 2016
 */
trait TypesafeConfigHelpers {
  def configFromFile(confFile: Path): Config = ConfigFactory.parseFile(confFile.toAbsolutePath.toFile)
}