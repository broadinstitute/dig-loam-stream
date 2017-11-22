package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory
import scala.util.Try
import loamstream.TestHelpers

/**
 * @author clint
 * Apr 20, 2017
 */
final class ExecutionConfigTest extends FunSuite {
  import ExecutionConfig.fromConfig
  import TestHelpers.path
  
  test("default") {
    assert(ExecutionConfig.default === ExecutionConfig(4, path("job-outputs"))) //scalastyle:ignore magic.number
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
    val expectedMaxRunsPerJob = 42 //scalastyle:ignore magic.number
    val expectedOutputDir = path("asdf/blah/foo")
    
    val input = s"""|loamstream { 
                    |  execution { 
                    |    maxRunsPerJob = $expectedMaxRunsPerJob 
                    |    outputDir = $expectedOutputDir
                    |  } 
                    |}""".stripMargin
    
    val executionConfig = Try(ConfigFactory.parseString(input)).flatMap(fromConfig).get
    
    assert(executionConfig === ExecutionConfig(expectedMaxRunsPerJob, expectedOutputDir))
  }
}
