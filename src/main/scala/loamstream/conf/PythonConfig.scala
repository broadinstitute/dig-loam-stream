package loamstream.conf

import java.nio.file.Path

import com.typesafe.config.Config

import scala.util.Try
import java.nio.file.Paths

/**
 * @author clint
 * Mar 28, 2017
 */
final case class PythonConfig(binary: Path = Paths.get("python"))

object PythonConfig {

  def fromConfig(config: Config): Try[PythonConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader

    //NB: Ficus now marshals the contents of loamstream.python into a PythonConfig instance.
    //Names of fields in PythonConfig and keys under loamstream.python must match.
    Try(config.as[PythonConfig]("loamstream.python"))
  }
}
