package loamstream.conf

import java.nio.file.Path
import com.typesafe.config.Config
import scala.util.Try
import SingularityConfig.Defaults

/**
 * @author clint
 * Sep 21, 2018
 */
final case class SingularityConfig(
    executable: String = Defaults.executable,
    mappedDirs: Seq[Path] = Defaults.mappedDirs,
    extraParams: String = Defaults.extraParams)

object SingularityConfig extends ConfigParser[SingularityConfig] {
  
  val default: SingularityConfig = SingularityConfig()
  
  object Defaults {
    val executable: String = "singularity"
    
    val mappedDirs: Seq[Path] = Nil
    
    val extraParams: String = ""
  }
  
  override def fromConfig(config: Config): Try[SingularityConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader
    
    //NB: Ficus now marshals the contents of loamstream.execution.singularity into a SingularityConfig instance.
    //Names of fields in SingularityConfig and keys under loamstream.execution.singularity must match.
    Try(config.as[SingularityConfig]("loamstream.execution.singularity"))
  }
}
