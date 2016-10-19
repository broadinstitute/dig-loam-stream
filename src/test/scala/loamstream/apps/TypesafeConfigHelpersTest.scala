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
    
    val config = Helpers.typesafeConfig(Paths.get("src/test/resources/foo.config"))
    
    //Config file should have been loaded and merged with defaults
    
    //overridden default 
    assert(config.getInt("loamstream.uger.maxNumJobs") === 42)
    
    //unchanged default
    assert(config.getString("loamstream.uger.logFile") === "uger.log")
    
    //new key
    assert(config.getString("loamstream.uger.foo") === "bar")
    
    //new key
    assert(config.getString("loamstream.nuh") === "zuh")
  }
}