package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.drm.uger.UgerDefaults
import loamstream.TestHelpers
import com.typesafe.config.ConfigFactory
import loamstream.conf.LoamConfig
import loamstream.conf.UgerConfig
import loamstream.conf.DrmConfig
import java.nio.file.Path
import loamstream.drm.Queue
import loamstream.conf.LsfConfig
import loamstream.conf.ConfigParser

/**
 * @author clint
 * Oct 16, 2017
 */
final class DrmSettingsTest extends FunSuite {
  
  test("fromUgerConfig") {
    doBasicFromConfigTest(UgerConfig.apply, DrmSettings.fromUgerConfig, Some(UgerDefaults.queue))
  }
  
  test("fromLsfConfig") {
    doBasicFromConfigTest(LsfConfig.apply, DrmSettings.fromLsfConfig, None)
  }
  
  private def doBasicFromConfigTest[C <: DrmConfig](
      makeConfig: (Path, Int, Cpus, Memory, CpuTime) => C, 
      makeSettings: C => DrmSettings,
      expectedQueue: Option[Queue]): Unit = {
    
    import TestHelpers.path
    
    val elevenJobs = 11
    val lotsOfCpus = Cpus(42)
    val seventeenGigs = Memory.inGb(17)
    val fiveHours = CpuTime.inHours(5)
    
    val config = makeConfig(path("/foo/bar"), elevenJobs, lotsOfCpus, seventeenGigs, fiveHours) 
      
    val settings = makeSettings(config)
    
    assert(settings.cores === lotsOfCpus)
    assert(settings.memoryPerCore === seventeenGigs)
    assert(settings.maxRunTime === fiveHours)
    assert(settings.queue === expectedQueue)
  }
  
  test("fromUgerConfig - parsed conf file") {
    doFromParsedConfFileTest("uger", UgerConfig, DrmSettings.fromUgerConfig, Some(UgerDefaults.queue))
  }
  
  test("fromLsfConfig - parsed conf file") {
    doFromParsedConfFileTest("lsf", LsfConfig, DrmSettings.fromLsfConfig, None)
  }
  
  private def doFromParsedConfFileTest[C <: DrmConfig](
      envType: String,
      confParser: ConfigParser[C], 
      makeSettings: C => DrmSettings,
      expectedQueue: Option[Queue]): Unit = {
    
    val configString = {
      s"""|loamstream {
          |  ${envType} {
          |    workDir = "/some/path"
          |    logFile = "uger.log"
          |    maxNumJobs = 4
          |    nativeSpecification = "-clear -cwd -shell y -b n -l"
          |    defaultCores = 42
          |    defaultMemoryPerCore = 17
          |    defaultMaxRunTime = 33
          |  }
          |}""".stripMargin
    }
    
    val config = confParser.fromConfig(ConfigFactory.parseString(configString)).get
    
    val settings = makeSettings(config)
    
    assert(settings.cores === Cpus(42))
    assert(settings.memoryPerCore === Memory.inGiB(17))
    assert(settings.maxRunTime === CpuTime.inHours(33))
    assert(settings.queue === expectedQueue)
  }
}
