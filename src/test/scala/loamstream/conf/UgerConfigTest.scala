package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.nio.file.Paths
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.drm.uger.UgerDefaults
import loamstream.TestHelpers

/**
 * @author clint
 * date: Jun 1, 2016
 */
final class UgerConfigTest extends FunSuite {
  import TestHelpers.path
  
  test("defaults") {
    val config = UgerConfig()
    
    assert(config.workDir === Locations.Default.ugerDir)
    assert(config.maxNumJobsPerTaskArray === UgerDefaults.maxNumJobsPerTaskArray)
    assert(config.defaultCores === UgerDefaults.cores)
    assert(config.defaultMemoryPerCore === UgerDefaults.memoryPerCore)
    assert(config.defaultMaxRunTime === UgerDefaults.maxRunTime)
    assert(config.extraPathDir === UgerDefaults.extraPathDir)
    assert(config.condaEnvName === UgerDefaults.condaEnvName)
    assert(config.staticJobSubmissionParams === UgerDefaults.staticJobSubmissionParams)
    assert(config.maxRetries === UgerDefaults.maxRetries)
  }
  
  test("Parsing a UgerConfig with all values provided should work") {
    val valid = ConfigFactory.parseString("""
      loamstream {
        uger {
          maxNumJobsPerTaskArray=44
          defaultCores = 42
          defaultMemoryPerCore = 9 // Gb
          defaultMaxRunTime = 11 // hours
          extraPathDir = /blah/baz
          condaEnvName = fooEnv
          staticJobSubmissionParams = "foo bar baz"
          maxRetries = 999
          maxQacctCacheSize = 123
        }
      }
      """)
      
    val config = UgerConfig.fromConfig(valid).get
    
    assert(config.workDir === Locations.Default.ugerDir)
    assert(config.maxNumJobsPerTaskArray === 44)
    assert(config.defaultCores === Cpus(42))
    assert(config.defaultMemoryPerCore=== Memory.inGb(9))
    assert(config.defaultMaxRunTime === CpuTime.inHours(11))
    assert(config.extraPathDir === path("/blah/baz"))
    assert(config.condaEnvName === "fooEnv")
    assert(config.staticJobSubmissionParams === "foo bar baz")
    assert(config.maxQacctCacheSize === 123)
    assert(config.maxRetries === 999)
  }
  
  test("Parsing a UgerConfig with optional values omitted should work") {
    val valid = ConfigFactory.parseString("""
      loamstream {
        uger {
          
        }
      }
      """)
      
    val config = UgerConfig.fromConfig(valid).get
    
    assert(config.workDir === Locations.Default.ugerDir)
    assert(config.maxNumJobsPerTaskArray === UgerDefaults.maxNumJobsPerTaskArray)
    assert(config.defaultCores === UgerDefaults.cores)
    assert(config.defaultMemoryPerCore === UgerDefaults.memoryPerCore)
    assert(config.defaultMaxRunTime === UgerDefaults.maxRunTime)
    assert(config.extraPathDir === UgerDefaults.extraPathDir)
    assert(config.condaEnvName === UgerDefaults.condaEnvName)
    assert(config.staticJobSubmissionParams === UgerDefaults.staticJobSubmissionParams)
    assert(config.maxQacctCacheSize === UgerDefaults.maxQacctCacheSize)
    assert(config.maxRetries === UgerDefaults.maxRetries)
  }
}
