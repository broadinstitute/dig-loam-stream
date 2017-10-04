package loamstream.util

import java.nio.file.Paths
import org.scalatest.FunSuite

/**
 * @author clint
 * Oct 19, 2016
 */
final class ConfigUtilsTest extends FunSuite {
  test("Loading a config file works") {
    //make sure we can access JVM system props
    val sysPropKey = "some.system.property"
    val sysPropValue = "skaldjalskdjklasdjlas"
    
    withSystemProperty(sysPropKey, sysPropValue) {
      val config = ConfigUtils.configFromFile(Paths.get("src/test/resources/foo.config"))
      
      //Config file should have been loaded BUT NOT merged with defaults
      
      //default from reference.conf, shouldn't have been loaded
      intercept[Exception] {
        config.getString("loamstream.uger.logFile")
      }
      
      //new key
      assert(config.getInt("loamstream.uger.maxNumJobs") === 42)
      
      //new key
      assert(config.getString("loamstream.uger.foo") === "bar")
      
      //new key
      assert(config.getString("loamstream.nuh") === "zuh")
      
      //system prop    
      assert(config.getString(sysPropKey) === sysPropValue)
    }
  }
  
  private def withSystemProperty[A](sysPropKey: String, sysPropValue: String)(f: => A): A = {
    def isUndefined(k: String): Boolean = Option(System.getProperty(k)).isEmpty
    
    assert(isUndefined(sysPropKey))
    
    System.setProperty(sysPropKey, sysPropValue)
    
    assert(System.getProperty(sysPropKey) === sysPropValue)
    
    try { f }
    finally {
      System.getProperties.remove(sysPropKey)
      
      assert(isUndefined(sysPropKey))
    }
  }
}
