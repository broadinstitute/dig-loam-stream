package loamstream.conf

import scala.util.Try

import org.scalatest.FunSuite

import com.typesafe.config.ConfigFactory

import loamstream.util.BashScript
import loamstream.util.Paths

/**
 * @author kyuksel
 *         date: 4/12/17
 */
final class RConfigTest extends FunSuite {
  import loamstream.TestHelpers.path
  
  private val binaryPath = path("path/to/R/binary")
  private val scriptDirPath = path("path/to/script/location")

  import RConfig.fromConfig
  
  test("fromConfig - defaults") {
    val confString =
      s"""loamstream {
            r {
              binary = "${BashScript.escapeString(binaryPath.toString)}"
            }
          }"""

    val config = ConfigFactory.parseString(confString)

    val rConfig = fromConfig(config).get

    assert(rConfig.binary === binaryPath)
    assert(rConfig.scriptDir === Paths.getCurrentDirectory)
  }

  test("fromConfig - defaults overridden") {
    val confString =
      s"""loamstream {
            r {
              binary = "${BashScript.escapeString(binaryPath.toString)}"
              scriptDir = "${BashScript.escapeString(scriptDirPath.toString)}"
            }
          }"""

    val config = ConfigFactory.parseString(confString)

    val rConfig = fromConfig(config).get

    assert(rConfig.binary === binaryPath)
    assert(rConfig.scriptDir === scriptDirPath)
  }

  test("fromConfig - bad input") {
    assert(fromConfig(ConfigFactory.empty).isFailure)

    def doTest(s: String): Unit = {
      val config = Try(ConfigFactory.parseString(s))

      assert(config.flatMap(fromConfig).isFailure)
    }

    doTest(null) //scalastyle:ignore null
    doTest("")
    doTest("asdsadasd")
    doTest("loamstream { }")
    doTest("loamstream { r { } }")
  }
}
