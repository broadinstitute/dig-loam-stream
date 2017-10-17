package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.uger.UgerDefaults
import loamstream.TestHelpers
import com.typesafe.config.ConfigFactory
import loamstream.conf.LoamConfig
import loamstream.conf.UgerConfig

/**
 * @author clint
 * Oct 16, 2017
 */
final class UgerSettingsTest extends FunSuite {
  test("UgerSettings constructor defaults") {
    {
      val lotsOfCpus = Cpus(42)
      val seventeenGigs = Memory.inGb(17)
      
      val withDefaults = UgerSettings(lotsOfCpus, seventeenGigs)
      
      assert(withDefaults.cores === lotsOfCpus)
      assert(withDefaults.memoryPerCore === seventeenGigs)
      assert(withDefaults.maxRunTime === UgerDefaults.maxRunTime)
      assert(withDefaults.queue === UgerDefaults.queue)
    }
    
    {
      val lotsOfCpus = Cpus(33)
      val nineteenGigs = Memory.inGb(19)
      val elevenHours = CpuTime.inHours(11)
    
      val withMaxRunTime = UgerSettings(lotsOfCpus, nineteenGigs, elevenHours)
      
      assert(withMaxRunTime.cores === lotsOfCpus)
      assert(withMaxRunTime.memoryPerCore === nineteenGigs)
      assert(withMaxRunTime.maxRunTime === elevenHours)
      assert(withMaxRunTime.queue === UgerDefaults.queue)
    }
  }
  
  test("from - defaults") {
    //NB: loamstream.uger block in loamstream-test.conf, loaded by TestHelpers.config, doesn't contain 
    //cores/mem/maxruntime params, so defaults will be used.
    val ugerConfig = TestHelpers.config.ugerConfig.get
    
    val settings = UgerSettings.from(ugerConfig)
    
    assert(settings.cores === UgerDefaults.cores)
    assert(settings.memoryPerCore === UgerDefaults.memoryPerCore)
    assert(settings.maxRunTime === UgerDefaults.maxRunTime)
    assert(settings.queue === UgerDefaults.queue)
  }
  
  test("from") {
    val configString = {
      """|loamstream {
         |  uger {
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
                        
    val ugerConfig = UgerConfig.fromConfig(ConfigFactory.parseString(configString)).get
    
    val settings = UgerSettings.from(ugerConfig)
    
    assert(settings.cores === Cpus(42))
    assert(settings.memoryPerCore === Memory.inGb(17))
    assert(settings.maxRunTime === CpuTime.inHours(33))
    assert(settings.queue === UgerDefaults.queue)
  }
}
