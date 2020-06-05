package loamstream.model.execute

import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.drm.Queue
import loamstream.drm.uger.UgerDefaults
import loamstream.conf.UgerConfig
import loamstream.conf.LsfConfig

import loamstream.drm.ContainerParams
import loamstream.googlecloud.ClusterConfig
import org.broadinstitute.dig.aws.emr.ClusterDef

/**
 * @author kyuksel
 * @author clint
 *         date: 3/7/17
 */
sealed abstract class Settings(val envType: EnvironmentType) {
  final def isLocal: Boolean = envType.isLocal

  final def isGoogle: Boolean = envType.isGoogle
  
  final def isUger: Boolean = envType.isUger
  
  final def isLsf: Boolean = envType.isLsf
}

/**
 * Execution-time settings for a group of 1 or more local jobs (no settings possible yet)
 */
final case object LocalSettings extends Settings(EnvironmentType.Local)

/**
 * Execution-time settings for a group of 1 or more Uger or LSF jobs 
 */
sealed abstract class DrmSettings(override val envType: EnvironmentType) extends Settings(envType) {
  def cores: Cpus
  def memoryPerCore: Memory
  def maxRunTime: CpuTime
  def queue: Option[Queue]
  def containerParams: Option[ContainerParams]
}

final case class UgerDrmSettings(
    cores: Cpus,
    memoryPerCore: Memory,
    maxRunTime: CpuTime,
    queue: Option[Queue],
    containerParams: Option[ContainerParams]) extends DrmSettings(EnvironmentType.Uger)
    
final case class LsfDrmSettings(
    cores: Cpus,
    memoryPerCore: Memory,
    maxRunTime: CpuTime,
    queue: Option[Queue],
    containerParams: Option[ContainerParams]) extends DrmSettings(EnvironmentType.Lsf)
    
object DrmSettings {
  type SettingsMaker = (Cpus, Memory, CpuTime, Option[Queue], Option[ContainerParams]) => DrmSettings
  
  type FieldsTuple = (Cpus, Memory, CpuTime, Option[Queue], Option[ContainerParams])
  
  def unapply(settings: DrmSettings): Option[FieldsTuple] = {
    import settings._
    
    Some((cores, memoryPerCore, maxRunTime, queue, containerParams))
  }
  
  def fromUgerConfig(config: UgerConfig): UgerDrmSettings = {
    UgerDrmSettings(
        config.defaultCores, 
        config.defaultMemoryPerCore, 
        config.defaultMaxRunTime, 
        Option(UgerDefaults.queue),
        None)
  }
  
  def fromLsfConfig(config: LsfConfig): LsfDrmSettings = {
    LsfDrmSettings(config.defaultCores, config.defaultMemoryPerCore, config.defaultMaxRunTime, None, None)
  }
}

/**
 * Execution-time settings for a google job 
 */
final case class GoogleSettings(
    clusterId: String, 
    clusterConfig: ClusterConfig) extends Settings(EnvironmentType.Google)

/**
 * Execution-time settings for an AWS job 
 */
final case class AwsSettings(clusterConfig: ClusterDef) extends Settings(EnvironmentType.Aws) {
  def clusterId: String = clusterConfig.name
}
