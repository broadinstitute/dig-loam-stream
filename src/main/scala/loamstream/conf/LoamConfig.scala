package loamstream.conf

import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.HailConfig
import scala.util.Try
import com.typesafe.config.Config
import scala.util.Success
import loamstream.util.Loggable

/**
 * @author clint
 * Feb 22, 2017
 */
final case class LoamConfig(
    ugerConfig: Option[UgerConfig],
    googleConfig: Option[GoogleCloudConfig],
    hailConfig: Option[HailConfig],
    pythonConfig: Option[PythonConfig],
    rConfig: Option[RConfig],
    executionConfig: ExecutionConfig)
    
object LoamConfig extends ConfigParser[LoamConfig] with Loggable {
  override def fromConfig(config: Config): Try[LoamConfig] = {
    val ugerConfig = UgerConfig.fromConfig(config)
    val googleConfig = GoogleCloudConfig.fromConfig(config)
    val hailConfig = HailConfig.fromConfig(config)
    val pythonConfig = PythonConfig.fromConfig(config)
    val rConfig = RConfig.fromConfig(config)
    val executionConfig = ExecutionConfig.fromConfig(config)

    if(executionConfig.isFailure) {
      debug(s"'loamstream.execution' section missing from config file, using defaults: ${ExecutionConfig.default}")
    }
    
    Success {
      LoamConfig( 
        ugerConfig.toOption,
        googleConfig.toOption,
        hailConfig.toOption,
        pythonConfig.toOption,
        rConfig.toOption,
        executionConfig.getOrElse(ExecutionConfig.default))
    }
  }
}
