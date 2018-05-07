package loamstream.model.execute

import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.drm.Queue
import loamstream.uger.UgerDefaults
import loamstream.conf.UgerConfig

/**
 * @author kyuksel
 * @author clint
 *         date: 3/7/17
 */

sealed trait Settings

final case object LocalSettings extends Settings

/**
 * Execution-time settings for a group of 1 or more Uger jobs 
 */
final case class UgerSettings(
    cores: Cpus,
    memoryPerCore: Memory,
    maxRunTime: CpuTime = UgerDefaults.maxRunTime,
    queue: Queue = UgerDefaults.queue) extends Settings
    
object UgerSettings {
  
  def from(ugerConfig: UgerConfig): UgerSettings = {
    import ugerConfig._
    
    UgerSettings(defaultCores, defaultMemoryPerCore, defaultMaxRunTime)
  }
}

/**
 * Execution-time settings for a google job 
 */
final case class GoogleSettings(cluster: String) extends Settings
