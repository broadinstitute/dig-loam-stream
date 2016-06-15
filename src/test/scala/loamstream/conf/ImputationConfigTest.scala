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
    val config = ImputationConfig.fromConfig(ConfigFactory.load("loamstream-test")).get

    // Single fields are correctly parsed
    assert(config.shapeIt.executable == Paths.get("/home/clint/bin/shapeit"))
    
    assert(config.impute2.executable == Paths.get("/home/clint/bin/impute2"))
    
    assert(config.shapeIt.script == Paths.get("/home/clint/workspace/imputation/shapeit.sh"))
    
    assert(config.shapeIt.workDir == Paths.get("/home/clint/workspace/imputation/shapeit_example"))
    
    assert(config.impute2.workDir == Paths.get("/home/clint/workspace/imputation/impute2_example"))
    
    val expectedNumThreads: Int = 4
    
    assert(config.shapeIt.numThreads == expectedNumThreads)

    // Composite fields are correctly formed
    assert(config.shapeIt.mapFile == 
        Paths.get("/home/clint/workspace/imputation/shapeit_example/genetic_map.txt.gz"))
  }

  test("Config objects are correctly parsed") {
    import com.typesafe.config.{Config, ConfigFactory}

    // Empty config object triggers failure
    assert(ImputationConfig.fromConfig(ConfigFactory.empty).isFailure)

    // Valid config is processed correctly
    val testConfFile = Paths.get("src/test/resources/loamstream-test.conf").toFile
    
    val config = ImputationConfig.fromConfig(ConfigFactory.parseFile(testConfFile)).get

    // Single fields are correctly parsed
    assert(config.shapeIt.script == Paths.get("/home/clint/workspace/imputation/shapeit.sh"))

    // Composite fields are correctly formed
    assert(config.shapeIt.mapFile == 
        Paths.get("/home/clint/workspace/imputation/shapeit_example/genetic_map.txt.gz"))
        
        
  }
}