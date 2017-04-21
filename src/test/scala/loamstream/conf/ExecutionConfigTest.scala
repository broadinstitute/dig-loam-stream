package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory
import scala.util.Try

/**
 * @author clint
 * Apr 20, 2017
 */
final class ExecutionConfigTest extends FunSuite {
  import ExecutionConfig.fromConfig
  
  test("default") {
    assert(ExecutionConfig.default === ExecutionConfig(4)) //scalastyle:ignore magic.number
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
    doTest("loamstream { execution { } }")
  }
  
  test("good input") {
    val expected = 42 //scalastyle:ignore magic.number
    val input = s"loamstream { execution { maxRunsPerJob = $expected } }"
    
    val executionConfig = Try(ConfigFactory.parseString(input)).flatMap(fromConfig).get
    
    assert(executionConfig === ExecutionConfig(expected))
  }
}
