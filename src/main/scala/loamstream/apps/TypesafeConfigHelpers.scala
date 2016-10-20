package loamstream.apps

import java.nio.file.Path

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * @author clint
 * Oct 19, 2016
 */
trait TypesafeConfigHelpers {
  def typesafeConfig(confFile: Path): Config = {
    //Load the file, and fall back to defaults for any keys that aren't present in the file 
    ConfigFactory.parseFile(confFile.toAbsolutePath.toFile).withFallback(ConfigFactory.load)
  }
}