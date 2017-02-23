package loamstream.googlecloud

import java.net.URI
import com.typesafe.config.Config
import scala.util.Try
import loamstream.conf.ValueReaders

/**
 * @author clint
 * Feb 22, 2017
 */
final case class HailConfig(jar: URI) 

object HailConfig {
  def fromConfig(config: Config): Try[HailConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.UriReader
    
    //NB: Ficus now marshals the contents of loamstream.googlecloud into a GoogleCloudConfig instance.
    //Names of fields in GoogleCloudConfig and keys under loamstream.googlecloud must match.
    Try(config.as[HailConfig]("loamstream.googlecloud.hail"))
  }
}
