package loamstream.util

import java.nio.file.Path
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * @author clint
 * Oct 19, 2016
 */
object ConfigUtils {
  def configFromFile(confFile: Path): Config = {
    val fromFile = ConfigFactory.parseFile(confFile.toAbsolutePath.toFile)
    
    val withSystemProps = ConfigFactory.defaultOverrides() 
    
    fromFile.withFallback(withSystemProps).resolve
  }
}
