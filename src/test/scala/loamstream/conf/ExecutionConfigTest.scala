package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory
import scala.util.Try
import loamstream.TestHelpers
import loamstream.util.BashScript.Implicits._
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

    val expected = ExecutionConfig(
        Defaults.executionPollingFrequencyInHz,
        Defaults.maxRunsPerJob,
        Defaults.maxWaitTimeForOutputs,
        Defaults.outputPollingFrequencyInHz,
        Defaults.dryRunOutputFile,
        Defaults.anonStoreDir,
        Defaults.singularityConfig,
        Defaults.dbDir,
        Defaults.logDir,
        Defaults.jobDataDir,
        Defaults.maxJobLogFilesPerDir)

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
    val expectedJobOutputDir = path("asdf/blah/foo")

    import scala.concurrent.duration._
    val expectedMaxWaitTime = 99.seconds
    val expectedOutputPollingFrequencyInHz = 1.23
    val expectedDryRunOutputFile = path("foo/asdkas/asdasd/asd.txt")
    val expectedAnonStoreDir = path("/alskdj/asd/asd")
    val expectedMaxJobLogFilesPerDir = 12345
    
    val input = s"""|loamstream {
                    |  execution {
                    |    executionPollingFrequencyInHz = 0.1
                    |    maxRunsPerJob = $expectedMaxRunsPerJob
                    |    maxWaitTimeForOutputs = "99 seconds"
                    |    outputPollingFrequencyInHz = 1.23
                    |    anonStoreDir = ${expectedAnonStoreDir.render}
                    |    maxJobLogFilesPerDir = $expectedMaxJobLogFilesPerDir
                    |  }
                    |}""".stripMargin

    val executionConfig = parse(input).get

    val expected = ExecutionConfig(
        executionPollingFrequencyInHz = 0.1,
        maxRunsPerJob = expectedMaxRunsPerJob,
        maxWaitTimeForOutputs = expectedMaxWaitTime,
        outputPollingFrequencyInHz = expectedOutputPollingFrequencyInHz,
        anonStoreDir = expectedAnonStoreDir,
        singularity = SingularityConfig.default,
        maxJobLogFilesPerDir = expectedMaxJobLogFilesPerDir)

    assert(executionConfig === expected)
  }
  
  test("good input - with singularity config") {
    val expectedMaxRunsPerJob = 42
    val expectedJobOutputDir = path("asdf/blah/foo")

    import scala.concurrent.duration._
    val expectedMaxWaitTime = 99.seconds
    val expectedOutputPollingFrequencyInHz = 1.23
    val expectedDryRunOutputFile = path("foo/asdkas/asdasd/asd.txt")
    val expectedAnonStoreDir = path("/alskdj/asd/asd")
    val expectedSingularityExecutable = path("/foo/bar/baz")
    val mappedDir0 = path("/bar")
    val mappedDir1 = path("/baz/blerg/blip")
    
    val input = s"""|loamstream {
                    |  execution {
                    |    executionPollingFrequencyInHz = 0.1
                    |    maxRunsPerJob = $expectedMaxRunsPerJob
                    |    maxWaitTimeForOutputs = "99 seconds"
                    |    outputPollingFrequencyInHz = 1.23
                    |    anonStoreDir = ${expectedAnonStoreDir.render}
                    |    singularity {
                    |      executable = ${expectedSingularityExecutable.render}
                    |      mappedDirs = [${mappedDir0.render}, ${mappedDir1.render}]
                    |    }
                    |  }
                    |}""".stripMargin

    val executionConfig = parse(input).get

    val expected = ExecutionConfig(
        executionPollingFrequencyInHz = 0.1,
        maxRunsPerJob = expectedMaxRunsPerJob,
        maxWaitTimeForOutputs = expectedMaxWaitTime,
        outputPollingFrequencyInHz = expectedOutputPollingFrequencyInHz,
        anonStoreDir = expectedAnonStoreDir,
        singularity = SingularityConfig(expectedSingularityExecutable.render, Seq(mappedDir0, mappedDir1)))

    assert(executionConfig === expected)
  }

  test("good input - no jobOutputDir") {
    val expectedMaxRunsPerJob = 42
    val expectedOutputPollingFrequencyInHz = ExecutionConfig.Defaults.outputPollingFrequencyInHz

    import scala.concurrent.duration._
    val expectedMaxWaitTime = 99.seconds

    val input = s"""|loamstream {
                    |  execution {
                    |    maxRunsPerJob = $expectedMaxRunsPerJob
                    |    maxWaitTimeForOutputs = "99 seconds"
                    |  }
                    |}""".stripMargin

    val executionConfig = parse(input).get

    val expected = ExecutionConfig(
        maxRunsPerJob = expectedMaxRunsPerJob,
        maxWaitTimeForOutputs = expectedMaxWaitTime,
        outputPollingFrequencyInHz = expectedOutputPollingFrequencyInHz)

    assert(executionConfig === expected)
  }

  test("good input - no maxRunsPerJob") {
    val expectedMaxRunsPerJob = ExecutionConfig.Defaults.maxRunsPerJob
    import scala.concurrent.duration._
    val expectedMaxWaitTime = 99.seconds
    val expectedOutputPollingFrequencyInHz = ExecutionConfig.Defaults.outputPollingFrequencyInHz

    val input = s"""|loamstream {
                    |  execution {
                    |    maxWaitTimeForOutputs = "99 seconds"
                    |  }
                    |}""".stripMargin

    val executionConfig = parse(input).get

    val expected = ExecutionConfig(
        maxRunsPerJob = expectedMaxRunsPerJob,
        maxWaitTimeForOutputs = expectedMaxWaitTime,
        outputPollingFrequencyInHz = expectedOutputPollingFrequencyInHz)

    assert(executionConfig === expected)
  }

  test("good input - all defaults") {
    val input = s"""|loamstream {
                    |  execution {
                    |  }
                    |}""".stripMargin

    val executionConfig = parse(input).get

    val expected = ExecutionConfig.default

    assert(executionConfig === expected)
  }

  private def parse(confString: String): Try[ExecutionConfig] = {
    Try(ConfigFactory.parseString(confString)).flatMap(fromConfig)
  }
}
