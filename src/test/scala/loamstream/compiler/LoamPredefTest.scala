package loamstream.compiler

import org.scalatest.FunSuite
import loamstream.loam.LoamScriptContext
import loamstream.conf.ExecutionConfig
import loamstream.conf.LoamConfig
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamCmdTool
import loamstream.TestHelpers
import loamstream.loam.LoamGraph
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.GoogleSettings
import loamstream.model.quantities.Memory
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.CpuTime
import loamstream.drm.uger.UgerDefaults
import loamstream.drm.DrmSystem
import loamstream.drm.lsf.LsfDefaults
import loamstream.model.execute.LsfDrmSettings
import loamstream.model.execute.UgerDrmSettings
import loamstream.drm.ContainerParams
import loamstream.drm.Queue
import loamstream.model.execute.Settings
import loamstream.model.execute.LocalSettings
import loamstream.googlecloud.ClusterConfig
import loamstream.conf.LsSettings
import loamstream.conf.SingularityConfig

/**
 * @author clint
 * May 5, 2017
 */
final class LoamPredefTest extends FunSuite {
  test("google - defaults") {
    
    implicit val scriptContext = newScriptContext
    
    assert(scriptContext.config.googleConfig.get.defaultClusterConfig === ClusterConfig.default)
    
    val settings = GoogleSettings(scriptContext.googleConfig.clusterId, ClusterConfig.default)
    
    doSettingsTest(scriptContext, LocalSettings, settings, LoamPredef.google)
  }
  
  test("google - non-defaults") {
    val baseConfig = TestHelpers.config
    
    val baseGoogleConfig = baseConfig.googleConfig.get
    
    val newClusterConfig = ClusterConfig.default.copy(numWorkers = 42)
    
    assert(newClusterConfig !== ClusterConfig.default)
    
    val loamConfig = {
      baseConfig.copy(googleConfig = Some(baseGoogleConfig.copy(defaultClusterConfig = newClusterConfig)))
    }
    
    assert(loamConfig !== baseConfig)
    
    implicit val scriptContext = new LoamScriptContext(LoamProjectContext.empty(loamConfig, LsSettings.noCliConfig))
    
    val settings = GoogleSettings(scriptContext.googleConfig.clusterId, newClusterConfig)
    
    doSettingsTest(scriptContext, LocalSettings, settings, LoamPredef.google)
  }
  
  test("googleWith") {
    
    val newClusterConfig = ClusterConfig.default.copy(numWorkers = 42)
    
    assert(newClusterConfig !== ClusterConfig.default)
    
    implicit val scriptContext = newScriptContext
    
    assert(scriptContext.config.googleConfig.get.defaultClusterConfig === ClusterConfig.default)
    
    val settings = GoogleSettings(scriptContext.googleConfig.clusterId, newClusterConfig)
    
    doSettingsTest(scriptContext, LocalSettings, settings, LoamPredef.googleWith(newClusterConfig))
  }
  
  test("local") {
    implicit val scriptContext = newScriptContext
    
    doSettingsTest(scriptContext, TestHelpers.defaultUgerSettings, LocalSettings, LoamPredef.local)
  }
  
  test("drm") {
    {
      implicit val scriptContext = newScriptContext(DrmSystem.Uger)
    
      doSettingsTest(scriptContext, LocalSettings, TestHelpers.defaultUgerSettings, LoamPredef.drm)
    }
    
    {
      implicit val scriptContext = newScriptContext(DrmSystem.Lsf)
    
      doSettingsTest(scriptContext, LocalSettings, TestHelpers.defaultLsfSettings, LoamPredef.drm)
    }
  }
  
  test("drmWith - defaults - Uger") {
    implicit val scriptContext = newScriptContext(DrmSystem.Uger)
    
    val ugerConfig = scriptContext.ugerConfig
    
    assert(ugerConfig.defaultCores !== UgerDefaults.cores)
    assert(ugerConfig.defaultMemoryPerCore !== UgerDefaults.memoryPerCore)
    assert(ugerConfig.defaultMaxRunTime !== UgerDefaults.maxRunTime)
    
    //Make sure defaults come from LoamConfig
    val expectedSettings = DrmSystem.Uger.settingsMaker(
        ugerConfig.defaultCores,
        ugerConfig.defaultMemoryPerCore,
        ugerConfig.defaultMaxRunTime,
        Option(UgerDefaults.queue),
        None)
    
    doSettingsTest(scriptContext, LocalSettings, expectedSettings, LoamPredef.drmWith())
  }
  
  test("drmWith - defaults - LSF") {
    implicit val scriptContext = newScriptContext(DrmSystem.Lsf)
    
    val lsfConfig = scriptContext.lsfConfig
    
    assert(lsfConfig.defaultCores !== LsfDefaults.cores)
    assert(lsfConfig.defaultMemoryPerCore !== LsfDefaults.memoryPerCore)
    assert(lsfConfig.defaultMaxRunTime !== LsfDefaults.maxRunTime)
    
    //Make sure defaults come from LoamConfig
    val expectedSettings = DrmSystem.Lsf.settingsMaker(
        lsfConfig.defaultCores,
        lsfConfig.defaultMemoryPerCore,
        lsfConfig.defaultMaxRunTime,
        None,
        None)
    
    doSettingsTest(scriptContext, LocalSettings, expectedSettings, LoamPredef.drmWith())
  }
  
  test("drmWith - non-defaults") {
    def doTest(drmSystem: DrmSystem): Unit = {
      implicit val scriptContext = newScriptContext(drmSystem)
      
      import TestHelpers.path
      
      val expectedSettings = drmSystem.settingsMaker(
          Cpus(2), 
          Memory.inGb(4), 
          CpuTime.inHours(6), 
          drmSystem.defaultQueue, //use this default, since it's not possible to specify a queue via drmWith()
          Some(ContainerParams(imageName = "library/foo:1.2.3", SingularityConfig.default.extraParams)))
      
      doSettingsTest(
          scriptContext, 
          LocalSettings, 
          expectedSettings, 
          LoamPredef.drmWith(2, 4, 6, "library/foo:1.2.3"))
    }
    
    doTest(DrmSystem.Lsf)
    doTest(DrmSystem.Uger)
  }
  
  private def newScriptContext: LoamScriptContext = new LoamScriptContext(TestHelpers.emptyProjectContext)
  
  private def newScriptContext(drmSystem: DrmSystem): LoamScriptContext = {
    new LoamScriptContext(TestHelpers.emptyProjectContext(drmSystem))
  }
  
  private def doSettingsTest[A](
      scriptContext: LoamScriptContext,
      initial: Settings, 
      shouldHaveSwitchedTo: Settings,
      switchSettings: (=> Any) => Any): Unit = {
    
    scriptContext.settings = initial
    
    assert(scriptContext.settings === initial)
    
    switchSettings {
      //We should have switched to the new EE
      assert(scriptContext.settings === shouldHaveSwitchedTo)
    }
    
    //We should have restored the original EE 
    assert(scriptContext.settings === initial)
  }
}
