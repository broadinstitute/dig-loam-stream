package loamstream.drm

import loamstream.conf.DrmConfig
import loamstream.conf.LsfConfig
import loamstream.conf.UgerConfig
import loamstream.drm.uger.UgerDefaults
import loamstream.loam.LoamScriptContext
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.Environment
import loamstream.model.execute.UgerDrmSettings
import loamstream.model.execute.LsfDrmSettings

/**
 * @author clint
 * May 23, 2018
 */
sealed trait DrmSystem {
  def name: String
  
  def makeEnvironment(settings: DrmSettings): Environment
  
  def defaultQueue: Option[Queue]
  
  def config(scriptContext: LoamScriptContext): DrmConfig
  
  def settingsFromConfig(scriptContext: LoamScriptContext): DrmSettings
  
  final def makeBasicEnvironment(scriptContext: LoamScriptContext) : Environment = {
    makeEnvironment(settingsFromConfig(scriptContext))
  }
  
  def settingsMaker: DrmSettings.SettingsMaker
}

object DrmSystem {
  final case object Uger extends DrmSystem {
    override def name: String = toString
    
    override def makeEnvironment(settings: DrmSettings): Environment = Environment.Uger(settings)
    
    override def defaultQueue: Option[Queue] = Option(UgerDefaults.queue)
    
    override def config(scriptContext: LoamScriptContext): UgerConfig = scriptContext.ugerConfig
  
    override def settingsFromConfig(scriptContext: LoamScriptContext): DrmSettings = {
      DrmSettings.fromUgerConfig(config(scriptContext))
    }
    
    override val settingsMaker: DrmSettings.SettingsMaker = UgerDrmSettings.apply
  }
  
  final case object Lsf extends DrmSystem {
    override def name: String = toString
    
    override def makeEnvironment(settings: DrmSettings): Environment = Environment.Lsf(settings)
    
    override def defaultQueue: Option[Queue] = None
    
    override def config(scriptContext: LoamScriptContext): LsfConfig = scriptContext.lsfConfig
    
    override def settingsFromConfig(scriptContext: LoamScriptContext): DrmSettings = {
      DrmSettings.fromLsfConfig(config(scriptContext))
    }
    
    override val settingsMaker: DrmSettings.SettingsMaker = LsfDrmSettings.apply
  }
  
  def fromName(name: String): Option[DrmSystem] = {
    val mungedName = name.toLowerCase.capitalize
    
    if(mungedName == Uger.name) { Some(Uger) }
    else if(mungedName == Lsf.name) { Some(Lsf) }
    else { None }
  }
  
  lazy val values: Iterable[DrmSystem] = Seq(Uger, Lsf)
}
