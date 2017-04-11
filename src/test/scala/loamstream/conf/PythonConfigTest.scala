package loamstream.conf

import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

import scala.util.Try

/**
 * @author kyuksel
 *         date: 4/11/17
 */
final class PythonConfigTest extends FunSuite {
  private val binaryPath = "path/to/python/binary"

  test("fromConfig - defaults") {
    val confString =
      s"""loamstream {
            python {
              binary = "$binaryPath"
            }
          }"""

    val config = ConfigFactory.parseString(confString)

    val pythonConfig = PythonConfig.fromConfig(config).get

    assert(pythonConfig.binary === Paths.get(binaryPath))
  }

  test("fromConfig - bad input") {
    val t = PythonConfig.fromConfig(ConfigFactory.empty)
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
