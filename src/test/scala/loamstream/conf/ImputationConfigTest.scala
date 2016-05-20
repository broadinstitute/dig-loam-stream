package loamstream.conf

import java.io.File

import org.scalatest.FunSuite

/**
  * @author kyuksel
  *         date: Mar 10, 2016
  */
final class ImputationConfigTest extends FunSuite {
  test("Config file is correctly parsed") {
    val config = ImputationConfig("loamstream-test.conf")

    // Single fields are correctly parsed
    assertResult("/humgen/diabetes/users/ryank/software/shapeit/bin/shapeit")(config.shapeItExecutable)
    val expectedNumThreads: Int = 16
    assertResult(expectedNumThreads)(config.shapeItNumThreads)

    // Composite fields are correctly formed
    assertResult("/humgen/diabetes/users/kyuksel/imputation/shapeit_example/genetic_map.txt.gz")(config.shapeItMapFile)
  }

  test("Config objects are correctly parsed") {
    import com.typesafe.config.{Config, ConfigFactory}

    // Empty config object triggers exception
    intercept[NoSuchElementException] {
      ImputationConfig(ConfigFactory.empty())
    }

    // Valid config is processed correctly
    val config = ImputationConfig(ConfigFactory.parseFile(new File("src/test/resources/loamstream-test.conf")))

    // Single fields are correctly parsed
    assertResult("/humgen/diabetes/users/kyuksel/imputation/shapeit_example/shapeit.sh")(config.shapeItScript)

    // Composite fields are correctly formed
    assertResult("/humgen/diabetes/users/kyuksel/imputation/shapeit_example/genetic_map.txt.gz")(config.shapeItMapFile)
  }
}