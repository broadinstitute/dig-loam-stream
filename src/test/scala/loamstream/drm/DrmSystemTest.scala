package loamstream.drm

import org.scalatest.FunSuite
import loamstream.drm.uger.UgerDefaults
import loamstream.model.execute.DrmSettings
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.model.execute.Environment
import loamstream.TestHelpers

/**
 * @author clint
 * May 25, 2018
 */
final class DrmSystemTest extends FunSuite {
  import DrmSystem.Uger
  import DrmSystem.Lsf
  
  test("defaultQueue") {
    assert(Uger.defaultQueue === Some(UgerDefaults.queue))
    assert(Lsf.defaultQueue === None)
  }
  
  test("makeEnvironment") {
    def makeSettings(drmSystem: DrmSystem) = drmSystem.settingsMaker(
        Cpus(42),
        Memory.inGb(11),
        CpuTime.inSeconds(12.34),
        Option(Queue("foo")),
        None)
        
    {
      val settings = makeSettings(Uger)
      
      assert(Uger.makeEnvironment(settings) === Environment.Uger(settings))
    }
    
    {
      val settings = makeSettings(Lsf)
      
      assert(Lsf.makeEnvironment(settings) === Environment.Lsf(settings))  
    }
  }
  
  test("config") {
    TestHelpers.withScriptContext { implicit scriptContext =>
      assert(Uger.config(scriptContext) === scriptContext.ugerConfig)
      assert(Lsf.config(scriptContext) === scriptContext.lsfConfig)
    }
  }
  
  test("settingsFromConfig") {
    TestHelpers.withScriptContext { implicit scriptContext =>
      assert(Uger.settingsFromConfig(scriptContext) === DrmSettings.fromUgerConfig(scriptContext.ugerConfig))
      assert(Lsf.settingsFromConfig(scriptContext) === DrmSettings.fromLsfConfig(scriptContext.lsfConfig))
    }
  }
  
  test("makeBasicEnvironment") {
    TestHelpers.withScriptContext { implicit scriptContext =>
      
      val expectedUger = Environment.Uger(DrmSettings.fromUgerConfig(scriptContext.ugerConfig))
      val expectedLsf = Environment.Lsf(DrmSettings.fromLsfConfig(scriptContext.lsfConfig))
      
      assert(Uger.makeBasicEnvironment(scriptContext) === expectedUger)
      
      assert(Lsf.makeBasicEnvironment(scriptContext) === expectedLsf)
    }
  }
}
