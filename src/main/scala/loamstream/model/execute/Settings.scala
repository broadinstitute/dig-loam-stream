package loamstream.model.execute

import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.uger.Queue
import loamstream.uger.UgerDefaults

/**
 * @author kyuksel
 * @author clint
 *         date: 3/7/17
 */

sealed trait Settings

final case object LocalSettings extends Settings

final case class UgerSettings(
    cores: Cpus,
    memoryPerCore: Memory,
    maxRunTime: CpuTime = UgerDefaults.maxRunTime,
    queue: Queue = UgerDefaults.queue) extends Settings
    
object UgerSettings {
  val Defaults: UgerSettings = UgerSettings(UgerDefaults.cores, UgerDefaults.memoryPerCore)
}

final case class GoogleSettings(cluster: String) extends Settings
