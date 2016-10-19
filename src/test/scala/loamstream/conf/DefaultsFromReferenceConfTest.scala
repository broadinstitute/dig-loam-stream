package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory

/**
 * @author clint
 * Oct 19, 2016
 */
final class DefaultsFromReferenceConfTest extends FunSuite {
  test("Defaults are loaded properly from reference.conf") {
    val config = ConfigFactory.load
    
    assert(config.getString("loamstream.uger.logFile") === "uger.log")
    assert(config.getInt("loamstream.uger.maxNumJobs") === 2400)
  }
}