package loamstream.util

import java.nio.file.Paths
import org.scalatest.FunSuite
import com.typesafe.config.Config
import loamstream.TestHelpers

/**
 * @author clint
 * Oct 19, 2016
 */
final class ConfigUtilsTest extends FunSuite {
  import TestHelpers.path
  
  test("Loading a config file works") {
    doTest(ConfigUtils.configFromFile(path("src/test/resources/foo.conf")), shouldHaveReferenceConfValues = false)
  }
  
  test("Loading a Config via prefix works") {
    doTest(ConfigUtils.configFromPrefix("foo"), shouldHaveReferenceConfValues = true)
  }
  
  test("Loading a Config from a string works") {
    val configData = Files.readFrom(path("src/test/resources/foo.conf"))
    
    doTest(ConfigUtils.configFromString(configData), shouldHaveReferenceConfValues = false)
  }
  
  private def doTest(config: Config, shouldHaveReferenceConfValues: Boolean): Unit = {
    //Config file should have been loaded, merged with system props, BUT NOT merged with defaults
      
    //default from reference.conf, shouldn't have been loaded
    
    if(shouldHaveReferenceConfValues) {
      assert(config.getString("loamstream.uger.logFile") === "uger.log")
    } else {
      intercept[Exception] {
        config.getString("loamstream.uger.logFile")
      }
    }
    
    intercept[Exception] {
      config.getString("loamstream.uger.blah")
    }
      
    //new key
    assert(config.getInt("loamstream.uger.maxNumJobs") === 42)
    
    //new key
    assert(config.getString("loamstream.uger.foo") === "bar")
    
    //new key
    assert(config.getString("loamstream.nuh") === "zuh")
    
    //system prop    
    assert(config.getString("user.dir") === System.getProperty("user.dir"))
  }
}
