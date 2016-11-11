package loamstream.apps

import java.nio.file.Paths

import org.scalatest.FunSuite

/**
 * @author clint
 * Oct 19, 2016
 */
final class TypesafeConfigHelpersTest extends FunSuite {
  test("Loading a config file works") {
    object Helpers extends TypesafeConfigHelpers
    
    val config = Helpers.configFromFile(Paths.get("src/test/resources/foo.config"))
    
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
  }
}