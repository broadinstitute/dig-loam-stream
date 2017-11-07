package loamstream.googlecloud

import java.net.URI
import java.nio.file.Path

import scala.util.Try

import com.typesafe.config.Config

import HailConfig.Defaults
import loamstream.conf.HasScriptDir
import loamstream.conf.ValueReaders
import loamstream.util.PathUtils


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
