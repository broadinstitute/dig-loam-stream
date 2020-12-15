package loamstream.loam.intake

import java.nio.file.Path
import loamstream.conf.ConfigParser
import scala.util.Try
import com.typesafe.config.Config
import loamstream.conf.ValueReaders

/**
 * @author clint
 * Mar 2, 2020
 */
final case class AggregatorIntakeConfig(
    condaEnvName: Option[String] = None,
    scriptsRoot: Path,
    condaExecutable: Path,
    genomeReferenceDir: Path,
    twentySixKIdMap: Path) {
  
}

object AggregatorIntakeConfig extends ConfigParser[AggregatorIntakeConfig] {
  override def fromConfig(config: Config): Try[AggregatorIntakeConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader
    
    //NB: Marshal the contents of loamstream.intake into a AggregatorIntakeConfig instance.
    //Names of fields in AggregatorIntakeConfig and keys under loamstream.intake must match.
    Try(config.as[AggregatorIntakeConfig]("loamstream.aggregator.intake"))
  }
}
