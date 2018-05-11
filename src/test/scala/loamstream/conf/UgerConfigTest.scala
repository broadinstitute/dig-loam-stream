package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.nio.file.Paths
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.drm.uger.UgerDefaults

/**
 * @author clint
 * date: Jun 1, 2016
 */
final class UgerConfigTest extends FunSuite {
  test("Parsing bad input should fail") {
    assert(UgerConfig.fromConfig(ConfigFactory.empty()).isFailure)
    
    assert(UgerConfig.fromConfig(ConfigFactory.load()).isFailure)
    
    assert(UgerConfig.fromConfig(ConfigFactory.parseString("{}")).isFailure)
  }
  
  test("Parsing a UgerConfig should work") {
    val valid = ConfigFactory.parseString("""
      loamstream {
        uger {
          workDir = "/foo/bar/baz"
          maxNumJobs=44
          defaultCores = 42
          defaultMemoryPerCore = 9 // Gb
          defaultMaxRunTime = 11 // hours
        }
      }
      """)
      
    val ugerConfig = UgerConfig.fromConfig(valid).get
    
    assert(ugerConfig.workDir === Paths.get("/foo/bar/baz"))
    assert(ugerConfig.maxNumJobs === 44)
    assert(ugerConfig.defaultCores === Cpus(42))
    assert(ugerConfig.defaultMemoryPerCore=== Memory.inGb(9))
    assert(ugerConfig.defaultMaxRunTime === CpuTime.inHours(11))
  }
  
  test("Parsing a UgerConfig with optional values omitted should work") {
    val valid = ConfigFactory.parseString("""
      loamstream {
        uger {
          workDir = "/foo/bar/baz"
          logFile = "nuh/zuh.log"
          maxNumJobs=44
        }
      }
      """)
      
    val ugerConfig = UgerConfig.fromConfig(valid).get
    
    assert(ugerConfig.workDir === Paths.get("/foo/bar/baz"))
    assert(ugerConfig.maxNumJobs === 44)
    assert(ugerConfig.defaultCores === UgerDefaults.cores)
    assert(ugerConfig.defaultMemoryPerCore=== UgerDefaults.memoryPerCore)
    assert(ugerConfig.defaultMaxRunTime === UgerDefaults.maxRunTime)
  }
}
