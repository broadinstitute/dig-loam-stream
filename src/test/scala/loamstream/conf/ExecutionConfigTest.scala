package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory
import scala.util.Try
import loamstream.TestHelpers
import java.nio.file.Path
import scala.util.Success

/**
 * @author clint
 * Apr 20, 2017
 */
final class ExecutionConfigTest extends FunSuite {
  import ExecutionConfig.fromConfig
  import TestHelpers.path
  
  test("default") {
    assert(ExecutionConfig.default === ExecutionConfig(4, path("job-outputs")))
  }
  
  test("fromConfig - bad input") {
    assert(fromConfig(ConfigFactory.empty) === Success(ExecutionConfig.default))

    def doTestShouldBeDefault(s: String): Unit = {
      val config = Try(ConfigFactory.parseString(s))

      assert(config.flatMap(fromConfig) === Success(ExecutionConfig.default))
    }
    
    def doTestShouldFail(s: String): Unit = {
      val config = Try(ConfigFactory.parseString(s))

      assert(config.flatMap(fromConfig).isFailure)
    }

    doTestShouldBeDefault("")
    doTestShouldFail("asdsadasd")
    doTestShouldBeDefault("loamstream { }")
  }
  
  test("good input") {
    val expectedMaxRunsPerJob = 42
    val expectedOutputDir = path("asdf/blah/foo")
    
    val input = s"""|loamstream { 
                    |  execution { 
                    |    maxRunsPerJob = $expectedMaxRunsPerJob 
                    |    outputDir = $expectedOutputDir
                    |  } 
                    |}""".stripMargin
    
    val executionConfig = parse(input).get
    
    assert(executionConfig === ExecutionConfig(expectedMaxRunsPerJob, expectedOutputDir))
  }
  
  test("good input - no outputDir") {
    val expectedMaxRunsPerJob = 42
    val expectedOutputDir = ExecutionConfig.Defaults.outputDir
    
    val input = s"""|loamstream { 
                    |  execution { 
                    |    maxRunsPerJob = $expectedMaxRunsPerJob 
                    |  } 
                    |}""".stripMargin
    
    val executionConfig = parse(input).get
    
    assert(executionConfig === ExecutionConfig(expectedMaxRunsPerJob, expectedOutputDir))
  }
  
  test("good input - no maxRunsPerJob") {
    val expectedMaxRunsPerJob = ExecutionConfig.Defaults.maxRunsPerJob
    val expectedOutputDir = path("asdf/blah/foo")
    
    val input = s"""|loamstream { 
                    |  execution { 
                    |    outputDir = $expectedOutputDir
                    |  } 
                    |}""".stripMargin
    
    val executionConfig = parse(input).get
    
    assert(executionConfig === ExecutionConfig(expectedMaxRunsPerJob, expectedOutputDir))
  }
  
  test("good input - all defaults") {
    val expectedMaxRunsPerJob = ExecutionConfig.Defaults.maxRunsPerJob
    val expectedOutputDir = ExecutionConfig.Defaults.outputDir
    
    val input = s"""|loamstream { 
                    |  execution { 
                    |  } 
                    |}""".stripMargin
    
    val executionConfig = parse(input).get
    
    assert(executionConfig === ExecutionConfig(expectedMaxRunsPerJob, expectedOutputDir))
  }
  
  private def parse(confString: String): Try[ExecutionConfig] = {
    Try(ConfigFactory.parseString(confString)).flatMap(fromConfig)
  }
}
