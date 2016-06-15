package loamstream.conf

import scala.util.Try

import com.typesafe.config.Config
import java.nio.file.Path
import loamstream.util.Files
import java.nio.file.Paths

/**
 * @author clint
 * date: Jun 13, 2016
 */
trait ConfigCompanion[C] {
  def fromConfig(config: Config): Try[C]
    
  final def fromFile(configFile: String): Try[C] = fromFile(Paths.get(configFile))
  
  final def fromFile(configFile: Path): Try[C] = {
    for {
      path <- Files.tryFile(configFile)
      config = TypesafeConfig.fromFile(path)
      result <- fromConfig(config)
    } yield result
  }
}