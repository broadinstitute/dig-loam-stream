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
   * Create a Config with values passed from the serialized `confData` as well as JVM system properties.  
   * System properties override values defined in `confData`.
   * @param confData the string containing HOCON to parse.
   * @return a Config object with values from `confData` as well as JVM system properties.
   */
  def configFromString(confData: String): Config = {
    //parse the config data.
    //NB: ConfigFactory.parseString() does not add fallbacks to JVM system properties like ConfigFactory.load() does.
    val fromString = ConfigFactory.parseString(confData)
    
    allowSyspropOverrides(fromString)
  }
  
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
