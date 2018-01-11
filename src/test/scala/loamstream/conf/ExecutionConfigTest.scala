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
    import ExecutionConfig.Defaults
    
    val expected = ExecutionConfig(Defaults.maxRunsPerJob, Defaults.outputDir, Defaults.maxWaitTimeForOutputs)
    
    assert(ExecutionConfig.default === expected)
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
    
    import scala.concurrent.duration._
    val expectedMaxWaitTime = 99.seconds
    
    val input = s"""|loamstream { 
                    |  execution { 
                    |    maxRunsPerJob = $expectedMaxRunsPerJob 
                    |    outputDir = $expectedOutputDir
                    |    maxWaitTimeForOutputs = "99 seconds"
                    |  } 
                    |}""".stripMargin
    
    val executionConfig = parse(input).get
    
    assert(executionConfig === ExecutionConfig(expectedMaxRunsPerJob, expectedOutputDir, expectedMaxWaitTime))
  }
  
  test("good input - no outputDir") {
    val expectedMaxRunsPerJob = 42
    val expectedOutputDir = ExecutionConfig.Defaults.outputDir

    import scala.concurrent.duration._
    val expectedMaxWaitTime = 99.seconds
    
    val input = s"""|loamstream { 
                    |  execution { 
                    |    maxRunsPerJob = $expectedMaxRunsPerJob 
                    |    maxWaitTimeForOutputs = "99 seconds"
                    |  } 
                    |}""".stripMargin
    
    val executionConfig = parse(input).get
    
    assert(executionConfig === ExecutionConfig(expectedMaxRunsPerJob, expectedOutputDir, expectedMaxWaitTime))
  }
  
  test("good input - no maxRunsPerJob") {
    val expectedMaxRunsPerJob = ExecutionConfig.Defaults.maxRunsPerJob
    val expectedOutputDir = path("asdf/blah/foo")
    import scala.concurrent.duration._
    val expectedMaxWaitTime = 99.seconds
    
    val input = s"""|loamstream { 
                    |  execution { 
                    |    outputDir = $expectedOutputDir
                    |    maxWaitTimeForOutputs = "99 seconds"
                    |  } 
                    |}""".stripMargin    
                    
    val executionConfig = parse(input).get
    
    assert(executionConfig === ExecutionConfig(expectedMaxRunsPerJob, expectedOutputDir, expectedMaxWaitTime))
  }
  
  test("good input - no maxWaitTimeForOutputs") {
    val expectedMaxRunsPerJob = 42
    val expectedOutputDir = path("asdf/blah/foo")
    val expectedMaxWaitTime = ExecutionConfig.Defaults.maxWaitTimeForOutputs
    
    val input = s"""|loamstream { 
                    |  execution { 
                    |    outputDir = $expectedOutputDir
                    |    maxRunsPerJob = $expectedMaxRunsPerJob
                    |  } 
                    |}""".stripMargin
    
    val executionConfig = parse(input).get
    
    assert(executionConfig === ExecutionConfig(expectedMaxRunsPerJob, expectedOutputDir, expectedMaxWaitTime))
  }
  
  test("good input - all defaults") {
    val expectedMaxRunsPerJob = ExecutionConfig.Defaults.maxRunsPerJob
    val expectedOutputDir = ExecutionConfig.Defaults.outputDir
    val expectedMaxWaitTime = ExecutionConfig.Defaults.maxWaitTimeForOutputs
    
    val input = s"""|loamstream { 
                    |  execution { 
                    |  } 
                    |}""".stripMargin
    
    val executionConfig = parse(input).get
    
    assert(executionConfig === ExecutionConfig(expectedMaxRunsPerJob, expectedOutputDir, expectedMaxWaitTime))
  }
  
  private def parse(confString: String): Try[ExecutionConfig] = {
    Try(ConfigFactory.parseString(confString)).flatMap(fromConfig)
  }
}
