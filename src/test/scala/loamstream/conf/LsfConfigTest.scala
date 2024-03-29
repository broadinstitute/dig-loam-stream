package loamstream.conf

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.drm.lsf.LsfDefaults
import com.typesafe.config.ConfigFactory
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory

/**
 * @author clint
 * Dec 14, 2018
 */
final class LsfConfigTest extends FunSuite {
  import TestHelpers.path
  
  test("defaults") {
    val config = LsfConfig()
    
    assert(config.workDir === Locations.Default.lsfDir)
    assert(config.maxNumJobsPerTaskArray === LsfDefaults.maxNumJobsPerTaskArray)
    assert(config.defaultCores === LsfDefaults.cores)
    assert(config.defaultMemoryPerCore === LsfDefaults.memoryPerCore)
    assert(config.defaultMaxRunTime === LsfDefaults.maxRunTime)
    assert(config.maxBacctRetries === LsfDefaults.maxBacctRetries)
    assert(config.maxRetries === LsfDefaults.maxRetries)
  }
  
  test("Parsing a UgerConfig with all values provided should work") {
    val valid = ConfigFactory.parseString("""
      loamstream {
        lsf {
          maxNumJobsPerTaskArray=44
          defaultCores = 42
          defaultMemoryPerCore = 9 // Gb
          defaultMaxRunTime = 11 // hours
          maxBacctRetries = 123
          maxRetries = 999
        }
      }
      """)
      
    val lsfConfig = LsfConfig.fromConfig(valid).get
    
    assert(lsfConfig.workDir === Locations.Default.lsfDir)
    assert(lsfConfig.maxNumJobsPerTaskArray === 44)
    assert(lsfConfig.defaultCores === Cpus(42))
    assert(lsfConfig.defaultMemoryPerCore=== Memory.inGb(9))
    assert(lsfConfig.defaultMaxRunTime === CpuTime.inHours(11))
    assert(lsfConfig.maxBacctRetries === 123)
    assert(lsfConfig.maxRetries === 999)
  }
  
  test("Parsing a UgerConfig with optional values omitted should work") {
    val valid = ConfigFactory.parseString("""
      loamstream {
        lsf {
          
        }
      }
      """)
      
    val lsfConfig = LsfConfig.fromConfig(valid).get
    
    assert(lsfConfig.workDir === Locations.Default.lsfDir)
    assert(lsfConfig.maxNumJobsPerTaskArray === LsfDefaults.maxNumJobsPerTaskArray)
    assert(lsfConfig.defaultCores === LsfDefaults.cores)
    assert(lsfConfig.defaultMemoryPerCore === LsfDefaults.memoryPerCore)
    assert(lsfConfig.defaultMaxRunTime === LsfDefaults.maxRunTime)
    assert(lsfConfig.maxBacctRetries === LsfDefaults.maxBacctRetries)
    assert(lsfConfig.maxRetries === LsfDefaults.maxRetries)
  }
}
