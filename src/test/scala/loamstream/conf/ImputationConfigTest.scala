package loamstream.conf

import java.io.File

import org.scalatest.FunSuite
import java.nio.file.Paths
import com.typesafe.config.ConfigFactory

/**
  * @author kyuksel
  *         date: Mar 10, 2016
  */
final class ImputationConfigTest extends FunSuite {
  test("Config file is correctly parsed") {
    val config = ImputationConfig(ConfigFactory.load("loamstream-test"))

    // Single fields are correctly parsed
    assert(config.shapeItExecutable == Paths.get("/humgen/diabetes/users/ryank/software/shapeit/bin/shapeit"))
    
    assert(config.shapeItScript == Paths.get("/humgen/diabetes/users/kyuksel/imputation/shapeit_example/shapeit.sh"))
    
    assert(config.shapeItWorkDir == Paths.get("/humgen/diabetes/users/kyuksel/imputation/shapeit_example"))
    
    val expectedNumThreads: Int = 16
    
    assert(config.shapeItNumThreads == expectedNumThreads)

    // Composite fields are correctly formed
    assert(config.shapeItMapFile == 
        Paths.get("/humgen/diabetes/users/kyuksel/imputation/shapeit_example/genetic_map.txt.gz"))
  }

  test("Config objects are correctly parsed") {
    import com.typesafe.config.{Config, ConfigFactory}

    // Empty config object triggers exception
    intercept[NoSuchElementException] {
      ImputationConfig(ConfigFactory.empty)
    }

    // Valid config is processed correctly
    val testConfFile = Paths.get("src/test/resources/loamstream-test.conf").toFile
    
    val config = ImputationConfig(ConfigFactory.parseFile(testConfFile))

    // Single fields are correctly parsed
    assert(config.shapeItScript == Paths.get("/humgen/diabetes/users/kyuksel/imputation/shapeit_example/shapeit.sh"))

    // Composite fields are correctly formed
    assert(config.shapeItMapFile == 
        Paths.get("/humgen/diabetes/users/kyuksel/imputation/shapeit_example/genetic_map.txt.gz"))
  }
}