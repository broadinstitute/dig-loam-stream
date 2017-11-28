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
   * Create a Config with values from `confFile` as well as JVM system properties.  System properties override
   * values defined in `confFile`.
   * @param confFile the path of the file to parse
   * @return a Config object with values from `confFile` as well as JVM system properties.
   */
  def configFromFile(confFile: Path): Config = {
    //parse the config file at `confFile`.
    //NB: ConfigFactory.parseFile() does not add fallbacks to JVM system properties like ConfigFactory.load() does.
    //We can't use the latter without naming config files according to load()'s conventions, and we want to be able
    //to parse arbitrary files. 
    val fromFile = ConfigFactory.parseFile(confFile.toAbsolutePath.toFile)
    
    allowSyspropOverrides(fromFile)
  }
  
  /**
   * Load a Config at `prefix` via ConfigFactory.load(), and return a Config object with values from the `load()`ed
   * Config, as well as JVM system properties.  System properties override values defined in the `load()`ed Config.
   * @param prefix the prefix of the config to load.  @see `com.typesafe.config.ConfigFactory.load(String)`  
   * @return a Config object with values from the Config at `prefix` as well as JVM system properties.
   */
  def configFromPrefix(prefix: String): Config = {
    //parse the config file at `confFile`.
    //NB: ConfigFactory.parseFile() does not add fallbacks to JVM system properties like ConfigFactory.load() does.
    //We can't use the latter without naming config files according to load()'s conventions, and we want to be able
    //to parse arbitrary files. 
    val fromPrefix = ConfigFactory.load(prefix)
    
    allowSyspropOverrides(fromPrefix)
  }
  
  /**
   * Returns a Config with all the values from `config` as well as JVM system properties.  System properties override
   * values defined in `config`.
   * @param config the Config object to allow overriding with system properties
   * @return a Config object with values from `config` as well as JVM system properties.
   */
  def allowSyspropOverrides(config: Config): Config = {
    //From the typesafe-config docs: Obtains the default override configuration, which currently consists of
    //system properties.
    val withSystemProps = ConfigFactory.defaultOverrides 
    
    withSystemProps.withFallback(config).resolve
  }
}
