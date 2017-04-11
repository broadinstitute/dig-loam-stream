package loamstream.conf

import java.nio.file.Path

import com.typesafe.config.Config

import scala.util.Try
import java.nio.file.Paths

/**
 * @author clint
 * Mar 28, 2017
 */
final case class RConfig(binary: Path = Paths.get("r"))

object RConfig {

  def fromConfig(config: Config): Try[RConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader

    //NB: Ficus now marshals the contents of loamstream.r into a RConfig instance.
    //Names of fields in RConfig and keys under loamstream.r must match.
    Try(config.as[RConfig]("loamstream.r"))
  }
}
