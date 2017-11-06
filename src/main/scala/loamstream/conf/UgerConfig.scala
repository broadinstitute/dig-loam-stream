package loamstream.conf

import java.nio.file.Path

import com.typesafe.config.Config

import scala.util.Try

/**
  * Created on: 5/4/16
  *
  * @author Kaan Yuksel
  */
final case class UgerConfig(workDir: Path, nativeSpecification: String, maxNumJobs: Int)

object UgerConfig extends ConfigParser[UgerConfig] {

  override def fromConfig(config: Config): Try[UgerConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader

    //NB: Ficus now marshals the contents of loamstream.uger into a UgerConfig instance.
    //Names of fields in UgerConfig and keys under loamstream.uger must match.
    Try(config.as[UgerConfig]("loamstream.uger"))
  }
}
