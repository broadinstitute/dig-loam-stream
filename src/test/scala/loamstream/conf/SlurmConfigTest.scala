package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.nio.file.Paths
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.drm.slurm.SlurmDefaults
import loamstream.TestHelpers

/**
 * @author clint
 * date: Jun 1, 2016
 */
final class SlurmConfigTest extends FunSuite {
  import TestHelpers.path
  
  test("defaults") {
    val config = SlurmConfig()
    
    assert(config.workDir === Locations.Default.slurmDir)
    assert(config.maxNumJobsPerTaskArray === SlurmDefaults.maxNumJobsPerTaskArray)
    assert(config.defaultCores === SlurmDefaults.cores)
    assert(config.defaultMemoryPerCore === SlurmDefaults.memoryPerCore)
    assert(config.defaultMaxRunTime === SlurmDefaults.maxRunTime)
    assert(config.maxRetries === SlurmDefaults.maxRetries)
    assert(config.maxSacctRetries === SlurmDefaults.maxRetries)
  }
  
  test("Parsing a SlurmConfig with all values provided should work") {
    val valid = ConfigFactory.parseString("""
      loamstream {
        slurm {
          maxNumJobsPerTaskArray=44
          defaultCores = 42
          defaultMemoryPerCore = 9 // Gb
          defaultMaxRunTime = 11 // hours
          maxRetries = 999
          maxSacctRetries = 123
        }
      }
      """)
      
    val config = SlurmConfig.fromConfig(valid).get
    
    assert(config.workDir === Locations.Default.slurmDir)
    assert(config.maxNumJobsPerTaskArray === 44)
    assert(config.defaultCores === Cpus(42))
    assert(config.defaultMemoryPerCore=== Memory.inGb(9))
    assert(config.defaultMaxRunTime === CpuTime.inHours(11))
    assert(config.maxRetries === 999)
    assert(config.maxSacctRetries === 123)
  }
  
  test("Parsing a SlurmConfig with optional values omitted should work") {
    val valid = ConfigFactory.parseString("""
      loamstream {
        slurm {
          
        }
      }
      """)
      
    val config = SlurmConfig.fromConfig(valid).get
    
    assert(config.workDir === Locations.Default.slurmDir)
    assert(config.maxNumJobsPerTaskArray === SlurmDefaults.maxNumJobsPerTaskArray)
    assert(config.defaultCores === SlurmDefaults.cores)
    assert(config.defaultMemoryPerCore === SlurmDefaults.memoryPerCore)
    assert(config.defaultMaxRunTime === SlurmDefaults.maxRunTime)
    assert(config.maxRetries === SlurmDefaults.maxRetries)
  }
}