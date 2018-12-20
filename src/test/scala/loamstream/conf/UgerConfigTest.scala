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
    
    assert(config.workDir === Locations.ugerDir)
    assert(config.scriptDir === Locations.ugerScriptDir)
    assert(config.maxNumJobs === UgerDefaults.maxConcurrentJobs)
    assert(config.defaultCores === UgerDefaults.cores)
    assert(config.defaultMemoryPerCore === UgerDefaults.memoryPerCore)
    assert(config.defaultMaxRunTime === UgerDefaults.maxRunTime)
    assert(config.extraPathDir === UgerDefaults.extraPathDir)
    assert(config.condaEnvName === UgerDefaults.condaEnvName)
    assert(config.staticJobSubmissionParams === UgerDefaults.staticJobSubmissionParams)
  }
  
  test("Parsing a UgerConfig with all values provided should work") {
    val valid = ConfigFactory.parseString("""
      loamstream {
        uger {
          maxNumJobs=44
          defaultCores = 42
          defaultMemoryPerCore = 9 // Gb
          defaultMaxRunTime = 11 // hours
          extraPathDir = /blah/baz
          condaEnvName = fooEnv
          staticJobSubmissionParams = "foo bar baz"
        }
      }
      """)
      
    val ugerConfig = UgerConfig.fromConfig(valid).get
    
    assert(ugerConfig.workDir === Locations.ugerDir)
    assert(ugerConfig.scriptDir === Locations.ugerScriptDir)
    assert(ugerConfig.maxNumJobs === 44)
    assert(ugerConfig.defaultCores === Cpus(42))
    assert(ugerConfig.defaultMemoryPerCore=== Memory.inGb(9))
    assert(ugerConfig.defaultMaxRunTime === CpuTime.inHours(11))
    assert(ugerConfig.extraPathDir === path("/blah/baz"))
    assert(ugerConfig.condaEnvName === "fooEnv")
    assert(ugerConfig.staticJobSubmissionParams === "foo bar baz")
  }
  
  test("Parsing a UgerConfig with optional values omitted should work") {
    val valid = ConfigFactory.parseString("""
      loamstream {
        uger {
          
        }
      }
      """)
      
    val ugerConfig = UgerConfig.fromConfig(valid).get
    
    assert(ugerConfig.workDir === Locations.ugerDir)
    assert(ugerConfig.scriptDir === Locations.ugerScriptDir)
    assert(ugerConfig.maxNumJobs === UgerDefaults.maxConcurrentJobs)
    assert(ugerConfig.defaultCores === UgerDefaults.cores)
    assert(ugerConfig.defaultMemoryPerCore === UgerDefaults.memoryPerCore)
    assert(ugerConfig.defaultMaxRunTime === UgerDefaults.maxRunTime)
    assert(ugerConfig.extraPathDir === UgerDefaults.extraPathDir)
    assert(ugerConfig.condaEnvName === UgerDefaults.condaEnvName)
    assert(ugerConfig.staticJobSubmissionParams === UgerDefaults.staticJobSubmissionParams)
  }
}
