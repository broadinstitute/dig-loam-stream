package loamstream.conf

import java.nio.file.Path

import com.typesafe.config.Config

import scala.util.Try
import java.nio.file.Paths

/**
 * @author clint
 * Mar 28, 2017
 */
final case class BashConfig(binary: Path = Paths.get("bash"))

object BashConfig {

  def fromConfig(config: Config): Try[BashConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader

    //NB: Ficus now marshals the contents of loamstream.bash into a BashConfig instance.
    //Names of fields in BashConfig and keys under loamstream.bash must match.
    Try(config.as[BashConfig]("loamstream.bash"))
  }
}
