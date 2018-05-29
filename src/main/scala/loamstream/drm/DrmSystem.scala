package loamstream.drm

import loamstream.conf.DrmConfig
import loamstream.conf.LsfConfig
import loamstream.conf.UgerConfig
import loamstream.drm.uger.UgerDefaults
import loamstream.loam.LoamScriptContext
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.Environment

/**
 * @author clint
 * May 23, 2018
 */
sealed trait DrmSystem {
  def makeEnvironment(settings: DrmSettings): Environment
  
  def defaultQueue: Option[Queue]
  
  def config(scriptContext: LoamScriptContext): DrmConfig
  
  def settingsFromConfig(scriptContext: LoamScriptContext): DrmSettings
  
  final def makeBasicEnvironment(scriptContext: LoamScriptContext) : Environment = {
    makeEnvironment(settingsFromConfig(scriptContext))
  }
}

object DrmSystem {
  final case object Uger extends DrmSystem {
    override def makeEnvironment(settings: DrmSettings): Environment = Environment.Uger(settings)
    
    override def defaultQueue: Option[Queue] = Option(UgerDefaults.queue)
    
    override def config(scriptContext: LoamScriptContext): UgerConfig = scriptContext.ugerConfig
  
    override def settingsFromConfig(scriptContext: LoamScriptContext): DrmSettings = {
      DrmSettings.fromUgerConfig(config(scriptContext))
    }
  }
  
  final case object Lsf extends DrmSystem {
    override def makeEnvironment(settings: DrmSettings): Environment = Environment.Lsf(settings)
    
    override def defaultQueue: Option[Queue] = None
    
    override def config(scriptContext: LoamScriptContext): LsfConfig = scriptContext.lsfConfig
    
    override def settingsFromConfig(scriptContext: LoamScriptContext): DrmSettings = {
      DrmSettings.fromLsfConfig(config(scriptContext))
    }
  }
}
