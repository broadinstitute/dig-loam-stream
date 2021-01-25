package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory
import scala.util.Try
import loamstream.TestHelpers

/**
 * @author clint
 * Jan 22, 2021
 */
final class SingularityConfigTest extends FunSuite {
  test("Defaults") {
    assert(SingularityConfig.default.executable === "singularity")
    assert(SingularityConfig.default.mappedDirs.isEmpty)
    assert(SingularityConfig.default.extraParams.isEmpty)
  }
  
  test("Empty HOCON") {
    def conf(input: String) = SingularityConfig.fromConfig(ConfigFactory.parseString(input)).get
    
    assert(conf("") === SingularityConfig.default)
    assert(conf("{}") === SingularityConfig.default)
  }
  
  test("Bad input") {
    def conf(input: String) = Try(ConfigFactory.parseString(input)).flatMap(SingularityConfig.fromConfig)
    
    assert(conf("asdasdasd").isFailure)
    assert(conf("{").isFailure)
  }
  
  test("Good input") {
    val input = """
loamstream {
  execution {
    singularity {
      executable = /foo/bar/blerg
      mappedDirs = [x, y, /bar/baz]
      extraParams = "--foo --bar 42"
    }
  }
}
"""
    
    val conf = SingularityConfig.fromConfig(ConfigFactory.parseString(input)).get
    
    import TestHelpers.path
    
    assert(conf.executable === "/foo/bar/blerg")
    assert(conf.mappedDirs === Seq(path("x"), path("y"), path("/bar/baz")))
    assert(conf.extraParams === "--foo --bar 42")
  }
}
