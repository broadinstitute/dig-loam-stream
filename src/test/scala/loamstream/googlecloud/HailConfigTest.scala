package loamstream.googlecloud

import org.scalatest.FunSuite
import java.net.URI
import com.typesafe.config.ConfigFactory
import java.nio.file.Paths

/**
 * @author clint
 * Feb 24, 2017
 */
final class HailConfigTest extends FunSuite {
  test("jarFile") {
    val jar = new URI("gs://foo/bar/baz")
    val zip = new URI("gs://blerg/zerg/flerg")
    
    assert(HailConfig(jar, zip).jarFile === "baz")
  }
  
  test("fromConfig - good input, defaults used") {
    val jar = new URI("gs://foo/bar/baz")
    val zip = new URI("gs://blerg/zerg/flerg")
    
    val configString = s"""
      loamstream {
        googlecloud {
          hail {
            jar = "$jar"
            zip = "$zip"
          }
        }
      }"""
            
    val actual = HailConfig.fromConfig(ConfigFactory.parseString(configString)).get
    
    assert(actual === HailConfig(jar, zip, HailConfig.Defaults.scriptDir))
  }
  
  test("fromConfig - good input, NO defaults used") {
    val jar = new URI("gs://foo/bar/baz")
    val zip = new URI("gs://blerg/zerg/flerg")
    val scriptDir = Paths.get("/foo/bar/baz")
    
    val configString = s"""
      loamstream {
        googlecloud {
          hail {
            jar = "$jar"
            zip = "$zip"
            scriptDir = "$scriptDir"
          }
        }
      }"""
            
    val actual = HailConfig.fromConfig(ConfigFactory.parseString(configString)).get
    
    assert(actual === HailConfig(jar, zip, scriptDir))
  }
  
  test("fromConfig - bad input") {
    def doTest(configString: String): Unit = {
      val attempt = HailConfig.fromConfig(ConfigFactory.parseString(configString))
    
      assert(attempt.isFailure)
    }
    
    doTest(s"""
        loamstream {
          googlecloud {
            hail {
              jar = "kasjdhkajsdh"
            }
          }
        }""")
    
    doTest(s"""
        loamstream {
          googlecloud {
            hail {
              zip = "kasjdhkajsdh"
            }
          }
        }""")
        
    doTest(s"""
        loamstream {
          googlecloud {
            hail {
              jar = "kasjdhkajsdh"
              zip = "kasjdhkajsdh"
            }
          }
        }""")
        
    doTest(s"""
        loamstream {
          googlecloud {
            hail {
              jar = ""
            }
          }
        }""")
        
    doTest(s"""
        loamstream {
          googlecloud {
            hail {
              zip = ""
            }
          }
        }""")
    
    doTest(s"""
        loamstream {
          googlecloud {
            hail {
              jar = ""
              zip = ""
            }
          }
        }""")
        
    doTest(s"""
        loamstream {
          googlecloud {
            hail {
              
            }
          }
        }""")
        
    doTest(s"""loamstream { }""")
    
    doTest("")
  }
}
