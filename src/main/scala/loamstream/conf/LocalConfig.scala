package loamstream.conf

import scala.concurrent.duration.Duration
import scala.util.Try

import com.typesafe.config.Config

import LocalConfig.Defaults
import loamstream.util.Loggable

/**
 * @author clint
 * Dec 21, 2017
 */
final case class LocalConfig(
    maxWaitTimeForOutputs: Duration = Defaults.maxWaitTimeForOutputs)

object LocalConfig extends ConfigParser[LocalConfig] with Loggable {

  object Defaults {
    import scala.concurrent.duration._
    
    val maxWaitTimeForOutputs: Duration = 30.seconds
  }
  
  override def fromConfig(config: Config): Try[LocalConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._

    trace("Parsing Uger config...")
    
    //NB: Ficus marshals the contents of loamstream.local into a LocalConfig instance.
    //Names of fields in LocalConfig and keys under loamstream.local must match.
    
    Try(config.as[LocalConfig]("loamstream.local"))
  }
}
