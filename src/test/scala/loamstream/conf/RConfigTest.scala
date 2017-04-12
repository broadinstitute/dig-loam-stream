package loamstream.conf

import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import loamstream.util.PathUtils
import org.scalatest.FunSuite

import scala.util.Try

/**
 * @author kyuksel
 *         date: 4/12/17
 */
final class RConfigTest extends FunSuite {
  private val binaryPath = Paths.get("path/to/R/binary")
  private val scriptDirPath = Paths.get("path/to/script/location")

  test("fromConfig - defaults") {
    val confString =
      s"""loamstream {
            r {
              binary = "$binaryPath"
            }
          }"""

    val config = ConfigFactory.parseString(confString)

    val rConfig = RConfig.fromConfig(config).get

    assert(rConfig.binary === binaryPath)
    assert(rConfig.scriptDir === PathUtils.getCurrentDirectory)
  }

  test("fromConfig - defaults overridden") {
    val confString =
      s"""loamstream {
            r {
              binary = "$binaryPath"
              scriptDir = "$scriptDirPath"
            }
          }"""

    val config = ConfigFactory.parseString(confString)

    val rConfig = RConfig.fromConfig(config).get

    assert(rConfig.binary === binaryPath)
    assert(rConfig.scriptDir === scriptDirPath)
  }

  test("fromConfig - bad input") {
    assert(RConfig.fromConfig(ConfigFactory.empty).isFailure)

    def doTest(s: String): Unit = {
      val config = Try(ConfigFactory.parseString(s))

      assert(config.flatMap(PythonConfig.fromConfig).isFailure)
    }

    doTest(null) //scalastyle:ignore null
    doTest("")
    doTest("asdsadasd")
    doTest("loamstream { }")
    doTest("loamstream { r { } }")
  }
}
