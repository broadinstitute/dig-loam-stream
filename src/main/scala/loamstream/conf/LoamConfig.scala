package loamstream.conf

import scala.util.Success
import scala.util.Try

import com.typesafe.config.Config

import loamstream.drm.DrmSystem
import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.HailConfig
import loamstream.util.Loggable

/**
 * @author clint
 * Feb 22, 2017
 */
final case class LoamConfig(
    ugerConfig: Option[UgerConfig],
    lsfConfig: Option[LsfConfig],
    slurmConfig: Option[SlurmConfig],
    googleConfig: Option[GoogleCloudConfig],
    hailConfig: Option[HailConfig],
    pythonConfig: Option[PythonConfig],
    rConfig: Option[RConfig],
    executionConfig: ExecutionConfig,
    compilationConfig: CompilationConfig,
    drmSystem: Option[DrmSystem] = None,
    cliConfig: Option[loamstream.cli.Conf] = None)
    
object LoamConfig extends ConfigParser[LoamConfig] with Loggable {
  val defaults: LoamConfig = fromString("{}").get
  
  override def fromConfig(config: Config): Try[LoamConfig] = {
    val ugerConfig = UgerConfig.fromConfig(config)
    val lsfConfig = LsfConfig.fromConfig(config)
    val slurmConfig = SlurmConfig.fromConfig(config)
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
        ugerConfig = ugerConfig.toOption,
        lsfConfig = lsfConfig.toOption,
        slurmConfig = slurmConfig.toOption,
        googleConfig = googleConfig.toOption,
        hailConfig = hailConfig.toOption,
        pythonConfig = pythonConfig.toOption,
        rConfig = rConfig.toOption,
        executionConfig = executionConfig.getOrElse(ExecutionConfig.default),
        compilationConfig = compilationConfig)
    }
  }
}
