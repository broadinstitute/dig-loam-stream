package loamstream.conf

import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.HailConfig

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
