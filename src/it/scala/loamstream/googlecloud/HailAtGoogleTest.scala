package loamstream.googlecloud

import java.nio.file.Files.exists
import scala.io.Source
import org.scalatest.FunSuite
import loamstream.apps.Main
import loamstream.util.CanBeClosed
import loamstream.util.{ Files => LFiles }
import loamstream.util.Paths.Implicits.PathHelpers
import loamstream.IntegrationTestHelpers
import org.scalactic.source.Position.apply
import org.scalatest.Finders

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
    
    val outputLines = CanBeClosed.enclosed(Source.fromFile(expectedOutput.toFile)) {
      _.getLines.map(_.trim).toIndexedSeq
    }
       
    assert(outputLines(0) === "AFR\tAMR\tEUR\tEAS\tSAS\tTOTAL")
    assert(outputLines(1) === "347\t901\t10045\t324\t75\t11692")
  }
}
