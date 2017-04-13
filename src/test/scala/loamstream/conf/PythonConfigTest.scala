package loamstream.conf

import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import loamstream.util.PathUtils
import org.scalatest.FunSuite

import scala.util.Try

/**
 * @author kyuksel
 *         date: 4/11/17
 */
final class PythonConfigTest extends FunSuite {
  private val binaryPath = Paths.get("path/to/python/binary")
  private val scriptDirPath = Paths.get("path/to/script/location")

  test("fromConfig - defaults") {
    val confString =
      s"""loamstream {
            python {
              binary = "$binaryPath"
            }
          }"""

    val config = ConfigFactory.parseString(confString)

    val pythonConfig = PythonConfig.fromConfig(config).get

    assert(pythonConfig.binary === binaryPath)
    assert(pythonConfig.scriptDir === PathUtils.getCurrentDirectory)
  }

  test("fromConfig - defaults overridden") {
    val confString =
      s"""loamstream {
            python {
              binary = "$binaryPath"
              scriptDir = "$scriptDirPath"
            }
          }"""

    val config = ConfigFactory.parseString(confString)

    val pythonConfig = PythonConfig.fromConfig(config).get

    assert(pythonConfig.binary === binaryPath)
    assert(pythonConfig.scriptDir === scriptDirPath)
  }

  test("fromConfig - bad input") {
    assert(PythonConfig.fromConfig(ConfigFactory.empty).isFailure)

    def doTest(s: String): Unit = {
      val config = Try(ConfigFactory.parseString(s))

      assert(config.flatMap(PythonConfig.fromConfig).isFailure)
    }

    doTest(null) //scalastyle:ignore null
    doTest("")
    doTest("asdsadasd")
    doTest("loamstream { }")
    doTest("loamstream { python { } }")
  }
}
