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
    assert(UgerConfig.apply(ConfigFactory.empty()).isFailure)
    
    assert(UgerConfig.apply(ConfigFactory.load()).isFailure)
    
    assert(UgerConfig.apply(ConfigFactory.parseString("{}")).isFailure)
    
    assert(UgerConfig.apply("asjdghjasdgjhasdg").isFailure)
  }
  
  test("Parsing a UgerConfig should work") {
    val valid = ConfigFactory.parseString("""
      loamstream {
        uger {
          workDir = "/foo/bar/baz"
          logFile = "nuh/zuh.log"
        }
      }
      """)
      
    val ugerConfig = UgerConfig(valid).get
    
    assert(ugerConfig.ugerWorkDir == Paths.get("/foo/bar/baz"))
    assert(ugerConfig.ugerLogFile == Paths.get("nuh/zuh.log"))
  }
}