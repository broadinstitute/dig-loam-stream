package loamstream.drm

import loamstream.conf.DrmConfig
import loamstream.conf.LsfConfig
import loamstream.conf.UgerConfig
import loamstream.drm.uger.UgerDefaults
import loamstream.loam.LoamScriptContext
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.UgerDrmSettings
import loamstream.model.execute.LsfDrmSettings

/**
 * @author clint
 * May 23, 2018
 */
sealed trait DrmSystem {
  type Config <: DrmConfig
  
  type Settings <: DrmSettings
  
  def name: String = toString
  
  def defaultQueue: Option[Queue]
  
  def config(scriptContext: LoamScriptContext): Config
  
  def settingsFromConfig(scriptContext: LoamScriptContext): Settings
  
  def settingsMaker: DrmSettings.SettingsMaker
}

object DrmSystem {
  final case object Uger extends DrmSystem {
    override type Config = UgerConfig
  
    override type Settings = UgerDrmSettings
    
    override def defaultQueue: Option[Queue] = Option(UgerDefaults.queue)
    
    override def config(scriptContext: LoamScriptContext): UgerConfig = scriptContext.ugerConfig
  
    override def settingsFromConfig(scriptContext: LoamScriptContext): Settings = {
      DrmSettings.fromUgerConfig(config(scriptContext))
    }
    
    override val settingsMaker: DrmSettings.SettingsMaker = UgerDrmSettings.apply
  }
  
  final case object Lsf extends DrmSystem {
    override type Config = LsfConfig
  
    override type Settings = LsfDrmSettings
    
    override def defaultQueue: Option[Queue] = None
    
    override def config(scriptContext: LoamScriptContext): LsfConfig = scriptContext.lsfConfig
    
    override def settingsFromConfig(scriptContext: LoamScriptContext): Settings = {
      DrmSettings.fromLsfConfig(config(scriptContext))
    }
    
    override val settingsMaker: DrmSettings.SettingsMaker = LsfDrmSettings.apply
  }
  
  final case object Slurm extends DrmSystem {
    override type Config = Nothing //TODO: SlurmConfig
  
    override type Settings = Nothing //TODO: SlurmDrmSettings
    
    override def defaultQueue: Option[Queue] = None
    
    override def config(scriptContext: LoamScriptContext): /*SlurmConfig*/ Nothing= ???//TODO scriptContext.slurmConfig
    
    override def settingsFromConfig(scriptContext: LoamScriptContext): Settings = {
      ???
      //TODO:
      //DrmSettings.fromSlurmConfig(config(scriptContext))
    }
    
    override val settingsMaker: DrmSettings.SettingsMaker = ???//TODO SlurmDrmSettings.apply
  }
  
  def fromName(name: String): Option[DrmSystem] = {
    val mungedName = name.toLowerCase.capitalize
    
    if(mungedName == Uger.name) { Some(Uger) }
    else if(mungedName == Lsf.name) { Some(Lsf) }
    else { None }
  }
  
  lazy val values: Iterable[DrmSystem] = Seq(Uger, Lsf)
}
