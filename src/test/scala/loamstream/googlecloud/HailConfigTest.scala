package loamstream.googlecloud

import org.scalatest.FunSuite
import java.net.URI
import com.typesafe.config.ConfigFactory

/**
 * @author clint
 * Feb 24, 2017
 */
final class HailConfigTest extends FunSuite {
  test("fromConfig - good input") {
    val uri = new URI("gs://foo/bar/baz")
    
    val configString = s"""
      loamstream {
        googlecloud {
          hail {
            jar = "$uri"
          }
        }
      }"""
            
    val actual = HailConfig.fromConfig(ConfigFactory.parseString(configString)).get
    
    assert(actual === HailConfig(uri))
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
              jar = ""
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
