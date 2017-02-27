package loamstream.conf

import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.HailConfig
import com.typesafe.config.Config
import scala.util.Try

/**
 * @author clint
 * Feb 22, 2017
 */
final case class LoamConfig(
    ugerConfig: Option[UgerConfig],
    googleConfig: Option[GoogleCloudConfig],
    hailConfig: Option[HailConfig])
