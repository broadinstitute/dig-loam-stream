package loamstream.conf

import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.HailConfig
import scala.util.Try
import com.typesafe.config.Config
import scala.util.Success
import loamstream.util.Loggable
import loamstream.util.Tries
import loamstream.drm.DrmSystem
import scala.collection.SortedMap.Default

/**
 * @author clint
 * Feb 22, 2017
 */
final case class LoamConfig(
    ugerConfig: Option[UgerConfig],
    lsfConfig: Option[LsfConfig],
    googleConfig: Option[GoogleCloudConfig],
    hailConfig: Option[HailConfig],
    pythonConfig: Option[PythonConfig],
    rConfig: Option[RConfig],
    executionConfig: ExecutionConfig,
    compilationConfig: CompilationConfig,
    drmSystem: Option[DrmSystem] = None)
    
object LoamConfig extends ConfigParser[LoamConfig] with Loggable {
  override def fromConfig(config: Config): Try[LoamConfig] = {
    val ugerConfig = UgerConfig.fromConfig(config)
    val lsfConfig = LsfConfig.fromConfig(config)
    val googleConfig = GoogleCloudConfig.fromConfig(config)
    val hailConfig = HailConfig.fromConfig(config)
    val pythonConfig = PythonConfig.fromConfig(config)
    val rConfig = RConfig.fromConfig(config)
    val executionConfig = ExecutionConfig.fromConfig(config)
    val compilationConfig = CompilationConfig.default

    if(executionConfig.isFailure) {
      debug(s"'loamstream.execution' section missing from config file, using defaults: ${ExecutionConfig.default}")
    }
    
    Success {
      LoamConfig(
        ugerConfig.toOption,
        lsfConfig.toOption,
        googleConfig.toOption,
        hailConfig.toOption,
        pythonConfig.toOption,
        rConfig.toOption,
        executionConfig.getOrElse(ExecutionConfig.default),
        compilationConfig)
    }
  }
}
