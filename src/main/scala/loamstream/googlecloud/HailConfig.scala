package loamstream.googlecloud

import java.net.URI
import com.typesafe.config.Config
import scala.util.Try
import loamstream.conf.ValueReaders
import HailConfig.Defaults
import java.nio.file.Path
import loamstream.util.PathUtils
import loamstream.conf.HasScriptDir

/**
 * @author clint
 * Feb 22, 2017
 */
final case class HailConfig(jar: URI, zip: URI, scriptDir: Path = Defaults.scriptDir) extends HasScriptDir {
  def jarFile: String = jar.getPath.split("/").last
}

object HailConfig {
  object Defaults {
    val scriptDir: Path = PathUtils.getCurrentDirectory
  }
  
  def fromConfig(config: Config): Try[HailConfig] = {
    //NB: Import all Ficus typeclasses except the one for reading java.net.URIs, since we want our own handling of 
    //those by ValueReaders.GcsUriReader.
    import net.ceedubs.ficus.Ficus.{javaURIReader => _, _}
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.GcsUriReader
    import ValueReaders.PathReader
    
    //NB: Ficus now marshals the contents of loamstream.googlecloud.hail into a HailConfig instance.
    //Names of fields in HailConfig and keys under loamstream.googlecloud.hail must match.
    Try(config.as[HailConfig]("loamstream.googlecloud.hail"))
  }
}
