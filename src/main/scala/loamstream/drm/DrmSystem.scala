package loamstream.drm

import loamstream.conf.DrmConfig
import loamstream.conf.LsfConfig
import loamstream.conf.UgerConfig
import loamstream.drm.uger.UgerDefaults
import loamstream.loam.LoamScriptContext
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.UgerDrmSettings
import loamstream.model.execute.LsfDrmSettings
import loamstream.conf.SlurmConfig
import loamstream.model.execute.SlurmDrmSettings
import loamstream.model.execute.Resources

/**
 * @author clint
 * May 23, 2018
 */
sealed trait DrmSystem {
  type Config <: DrmConfig
  
  type Settings <: DrmSettings

  type Resources <: Resources.DrmResources
  
  def name: String = toString
  
  def defaultQueue: Option[Queue]
  
  def config(scriptContext: LoamScriptContext): Config
  
  def settingsFromConfig(scriptContext: LoamScriptContext): Settings
  
  def settingsMaker: DrmSettings.SettingsMaker

  def resourcesMaker: Resources.DrmResources.ResourcesMaker[Resources]
  
  def configKey: String = s"loamstream.${name.toLowerCase}"
  
  def format(drmTaskId: DrmTaskId): String = formatWithSeparator('.', drmTaskId) 
  
  final protected def formatWithSeparator(sep: Char, drmTaskId: DrmTaskId): String = {
    s"${drmTaskId.jobId}${sep}${drmTaskId.taskIndex}"
  }
}

object DrmSystem {
  final case object Uger extends DrmSystem {
    override type Config = UgerConfig
  
    override type Settings = UgerDrmSettings

    override type Resources = Resources.UgerResources
    
    override def defaultQueue: Option[Queue] = Option(UgerDefaults.queue)
    
    override def config(scriptContext: LoamScriptContext): UgerConfig = scriptContext.ugerConfig
  
    override def settingsFromConfig(scriptContext: LoamScriptContext): Settings = {
      DrmSettings.fromUgerConfig(config(scriptContext))
    }
    
    override val settingsMaker: DrmSettings.SettingsMaker = UgerDrmSettings.apply

    override val resourcesMaker: Resources.DrmResources.ResourcesMaker[Resources] = Resources.UgerResources.apply
  }
  
  final case object Lsf extends DrmSystem {
    override type Config = LsfConfig
  
    override type Settings = LsfDrmSettings

    override type Resources = Resources.LsfResources
    
    override def defaultQueue: Option[Queue] = None
    
    override def config(scriptContext: LoamScriptContext): LsfConfig = scriptContext.lsfConfig
    
    override def settingsFromConfig(scriptContext: LoamScriptContext): Settings = {
      DrmSettings.fromLsfConfig(config(scriptContext))
    }
    
    override val settingsMaker: DrmSettings.SettingsMaker = LsfDrmSettings.apply

    override val resourcesMaker: Resources.DrmResources.ResourcesMaker[Resources] = Resources.LsfResources.apply
  }
  
  final case object Slurm extends DrmSystem {
    override type Config = SlurmConfig
  
    override type Settings = SlurmDrmSettings

    override type Resources = Resources.SlurmResources
    
    override def defaultQueue: Option[Queue] = None
    
    override def config(scriptContext: LoamScriptContext): SlurmConfig = scriptContext.slurmConfig
    
    override def settingsFromConfig(scriptContext: LoamScriptContext): Settings = {
      DrmSettings.fromSlurmConfig(config(scriptContext))
    }
    
    override val settingsMaker: DrmSettings.SettingsMaker = SlurmDrmSettings.apply

    override val resourcesMaker: Resources.DrmResources.ResourcesMaker[Resources] = Resources.SlurmResources.apply
    
    override def format(drmTaskId: DrmTaskId): String = formatWithSeparator('_', drmTaskId)
  }
  
  def fromName(name: String): Option[DrmSystem] = byName.get(name.trim.toLowerCase)
  
  lazy val values: Iterable[DrmSystem] = Seq(Uger, Lsf, Slurm)
  
  private lazy val byName: Map[String, DrmSystem] = values.iterator.map(d => d.name.toLowerCase -> d).toMap
}
