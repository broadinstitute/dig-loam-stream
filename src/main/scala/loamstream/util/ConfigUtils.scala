package loamstream.util

import java.nio.file.Path
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * @author clint
 * Oct 19, 2016
 */
object ConfigUtils {
  /**
   * Create a Config with values from `confFile` as well as JVM system properties.
   * @param confFile the path of the file to parse
   * @return a Config object values from `confFile` as well as JVM system properties.
   */
  def configFromFile(confFile: Path): Config = {
    //parse the config file at `confFile`.
    //NB: ConfigFactory.parseFile() does not add fallbacks to JVM system properties like ConfigFactory.load() does.
    //We can't use the latter without naming config files according to load()'s conventions, and we want to be able
    //to parse arbitrary files. 
    val fromFile = ConfigFactory.parseFile(confFile.toAbsolutePath.toFile)
    
    //From the typesafe-config docs: Obtains the default override configuration, which currently consists of
    //system properties.
    val withSystemProps = ConfigFactory.defaultOverrides() 
    
    fromFile.withFallback(withSystemProps).resolve
  }
}
