package loamstream.googlecloud

import java.net.URI
import java.nio.file.Path

import scala.util.Try

import com.typesafe.config.Config

import HailConfig.Defaults
import loamstream.conf.HasScriptDir
import loamstream.conf.ValueReaders
import loamstream.util.Paths


/**
 * @author clint
 * Feb 22, 2017
 */
final case class HailConfig(
    condaEnv: String,
    scriptDir: Path = Defaults.scriptDir) extends HasScriptDir {
  
  def withScriptDir(newScriptDir: Path): HailConfig = copy(scriptDir = newScriptDir)
}

object HailConfig {
  object Defaults {
    val scriptDir: Path = Paths.getCurrentDirectory
  }
  
  def fromConfig(config: Config): Try[HailConfig] = {
    //NB: Import all Ficus typeclasses except the one for reading java.net.URIs, since without doing this, automatic 
    //unmarshalling fails.  (I'm not sure why :\ -Clint Jul 22, 2019) 
    import net.ceedubs.ficus.Ficus.{javaURIReader => _, _}
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader
    
    //NB: Ficus now marshals the contents of loamstream.googlecloud.hail into a HailConfig instance.
    //Names of fields in HailConfig and keys under loamstream.googlecloud.hail must match.
    Try(config.as[HailConfig]("loamstream.googlecloud.hail"))
  }
}
