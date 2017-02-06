package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.nio.file.Paths

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
          logFile = "nuh/zuh.log"
          maxNumJobs=44
        }
      }
      """)
      
    val ugerConfig = UgerConfig.fromConfig(valid).get
    
    assert(ugerConfig.workDir === Paths.get("/foo/bar/baz"))
    assert(ugerConfig.logFile === Paths.get("nuh/zuh.log"))
    assert(ugerConfig.maxNumJobs === 44)
  }
}
