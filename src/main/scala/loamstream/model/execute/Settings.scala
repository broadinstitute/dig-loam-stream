package loamstream.model.execute

import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.drm.Queue
import loamstream.drm.uger.UgerDefaults
import loamstream.conf.UgerConfig
import loamstream.conf.LsfConfig
import loamstream.conf.DrmConfig
import loamstream.drm.lsf.LsfDefaults
import loamstream.conf.LoamConfig
import scala.util.Try
import loamstream.util.Tries
import loamstream.drm.DrmSystem
import loamstream.util.Options
import loamstream.drm.DockerParams

/**
 * @author kyuksel
 * @author clint
 *         date: 3/7/17
 */

sealed trait Settings

/**
 * Execution-time settings for a group of 1 or more local jobs (no settings possible yet)
 */
final case object LocalSettings extends Settings

/**
 * Execution-time settings for a group of 1 or more Uger or LSF jobs 
 */
final case class DrmSettings(
    cores: Cpus,
    memoryPerCore: Memory,
    maxRunTime: CpuTime,
    queue: Option[Queue],
    dockerParams: Option[DockerParams]) extends Settings
    
object DrmSettings {
  def fromUgerConfig(config: UgerConfig): DrmSettings = {
    DrmSettings(
        config.defaultCores, 
        config.defaultMemoryPerCore, 
        config.defaultMaxRunTime, 
        Option(UgerDefaults.queue),
        None)
  }
  
  def fromLsfConfig(config: LsfConfig): DrmSettings = {
    DrmSettings(config.defaultCores, config.defaultMemoryPerCore, config.defaultMaxRunTime, None, None)
  }
}

/**
 * Execution-time settings for a google job 
 */
final case class GoogleSettings(cluster: String) extends Settings
