package loamstream.model.execute

import java.nio.file.Path

import org.scalatest.FunSuite

import com.typesafe.config.ConfigFactory

import loamstream.conf.ConfigParser
import loamstream.conf.DrmConfig
import loamstream.conf.LsfConfig
import loamstream.conf.UgerConfig
import loamstream.drm.Queue
import loamstream.drm.uger.UgerDefaults
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory

/**
 * @author clint
 * Oct 16, 2017
 */
final class DrmSettingsTest extends FunSuite {
  
  import loamstream.TestHelpers.path
  
  test("fromUgerConfig") {
    val elevenJobs = 11
    val lotsOfCpus = Cpus(42)
    val seventeenGigs = Memory.inGb(17)
    val fiveHours = CpuTime.inHours(5)
    val someDir = path("/some/dir")
    val someEnv = "someEnv"
    
    val config = UgerConfig(
        maxNumJobsPerTaskArray = elevenJobs, 
        defaultCores = lotsOfCpus, 
        defaultMemoryPerCore = seventeenGigs,
        defaultMaxRunTime = fiveHours, 
        extraPathDir = someDir,
        condaEnvName = someEnv) 
      
    val settings = DrmSettings.fromUgerConfig(config)
    
    assert(settings.cores === lotsOfCpus)
    assert(settings.memoryPerCore === seventeenGigs)
    assert(settings.maxRunTime === fiveHours)
    assert(settings.queue === Some(UgerDefaults.queue))
    assert(settings.containerParams === None)
  }
  
  test("fromLsfConfig") {
    val elevenJobs = 11
    val lotsOfCpus = Cpus(42)
    val seventeenGigs = Memory.inGb(17)
    val fiveHours = CpuTime.inHours(5)
    
    val config = LsfConfig(
        maxNumJobsPerTaskArray = elevenJobs, 
        defaultCores = lotsOfCpus, 
        defaultMemoryPerCore = seventeenGigs,
        defaultMaxRunTime = fiveHours) 
      
    val settings = DrmSettings.fromLsfConfig(config)
    
    assert(settings.cores === lotsOfCpus)
    assert(settings.memoryPerCore === seventeenGigs)
    assert(settings.maxRunTime === fiveHours)
    assert(settings.queue === None)
    assert(settings.containerParams === None)
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
    assert(settings.memoryPerCore === Memory.inGb(17))
    assert(settings.maxRunTime === CpuTime.inHours(33))
    assert(settings.queue === expectedQueue)
  }
}
