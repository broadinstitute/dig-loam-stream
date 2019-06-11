package loamstream

import org.scalatest.FunSuite
import loamstream.util.Paths.Implicits._
import loamstream.util.{Files => LFiles}
import loamstream.apps.Main
import java.nio.file.Files.exists

/**
 * @author clint
 * Jun 11, 2019
 */
final class HailAtGoogleTest extends FunSuite {
  test("Copy a file to Google, run a simple Hail job with it, and copy the result to the local FS.") {
    val workDir = IntegrationTestHelpers.getWorkDirUnderTarget().toAbsolutePath
    
    val testConfContents = s"""|googleCloudUri = "gs://loamstream/dev/integration_tests"
                               |workDir = "${workDir}"
                               |""".stripMargin.trim
                               
    val testConf = workDir / "test.conf"
    
    LFiles.writeTo(testConf)(testConfContents)
    
    System.setProperty("dataConfig", testConf.toString)

    val expectedOutput = workDir / "output.tsv"
    
    assert(exists(expectedOutput) === false)
    
    Main.main(Array(
        "--conf", "src/it/resources/hail-at-google/loamstream.conf",
        "--loams", "src/it/resources/hail-at-google/test.loam"))
        
    assert(exists(expectedOutput))
  }
}
