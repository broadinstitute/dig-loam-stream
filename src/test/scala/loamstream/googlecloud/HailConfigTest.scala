package loamstream.googlecloud

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite
import java.net.URI
import java.nio.file.Paths
import loamstream.util.BashScript.Implicits._

/**
 * @author clint
 * Feb 24, 2017
 */
final class HailConfigTest extends FunSuite {
  test("jarFile") {
    val jar = new URI("gs://foo/bar/baz")
    val zip = new URI("gs://blerg/zerg/flerg")
    val envName = "hail-0.2.18"

    assert(HailConfig(jar, zip, envName).jarFile === "baz")
  }

  test("fromConfig - good input, defaults used") {
    val jar = new URI("gs://foo/bar/baz")
    val zip = new URI("gs://blerg/zerg/flerg")
    val envName = "hail-0.2.18"

    val configString = s"""
      loamstream {
        googlecloud {
          hail {
            jar = "$jar"
            zip = "$zip"
            condaEnv = "$envName"
          }
        }
      }"""

    val actual = HailConfig.fromConfig(ConfigFactory.parseString(configString)).get

    assert(actual === HailConfig(jar, zip, envName, HailConfig.Defaults.scriptDir))
  }

  test("fromConfig - good input, NO defaults used") {
    val jar = new URI("gs://foo/bar/baz")
    val zip = new URI("gs://blerg/zerg/flerg")
    val envName = "hail-0.2.18"
    val scriptDir = Paths.get("/foo/bar/baz")

    val configString = s"""
      loamstream {
        googlecloud {
          hail {
            jar = "$jar"
            zip = "$zip"
            condaEnv = "$envName"
            scriptDir = "${scriptDir.render}"
          }
        }
      }"""

    val actual = HailConfig.fromConfig(ConfigFactory.parseString(configString)).get

    assert(actual === HailConfig(jar, zip, envName, scriptDir))
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
