package loamstream.googlecloud

import java.nio.file.Files.exists

import scala.io.Source

import org.scalatest.FunSuite

import loamstream.IntegrationTestHelpers
import loamstream.apps.Main
import loamstream.util.CanBeClosed
import loamstream.util.{ Files => LFiles }
import loamstream.util.Paths.Implicits.PathHelpers
import java.nio.file.Path


/**
 * @author clint
 * Jun 11, 2019
 */
final class HailAtGoogleMultipleClusterConfigsTest extends FunSuite {
  test("Copy a file to Google, run a simple Hail job with it, and copy the result to the local FS.") {
    val workDir = IntegrationTestHelpers.getWorkDirUnderTarget().toAbsolutePath
    
    val testConfContents = s"""|googleCloudUri = "gs://loamstream/dev/integration_tests/multi-clusterconfigs"
                               |workDir = "${workDir}"
                               |""".stripMargin.trim
                               
    val testConf = workDir / "test.conf"
    
    LFiles.writeTo(testConf)(testConfContents)
    
    System.setProperty("dataConfig", testConf.toString)

    val expectedOutputs = Seq(workDir / "output0.tsv", workDir / "output1.tsv")
    
    assert(exists(expectedOutputs(0)) === false)
    assert(exists(expectedOutputs(1)) === false)
    
    Main.main(Array(
        "--conf", "src/it/resources/hail-at-google-multiple-clusterconfigs/loamstream.conf",
        "--loams", "src/it/resources/hail-at-google-multiple-clusterconfigs/test.loam"))
    
    def getOutputLines(path: Path) = CanBeClosed.enclosed(Source.fromFile(path.toFile)) {
      _.getLines.map(_.trim).toIndexedSeq
    }
                               
    val outputLines0 = getOutputLines(expectedOutputs(0))
    val outputLines1 = getOutputLines(expectedOutputs(1))
       
    val expectedOutputLines = Seq(
        "AFR\tAMR\tEUR\tEAS\tSAS\tTOTAL",
        "347\t901\t10045\t324\t75\t11692")
                               
    assert(outputLines0 === expectedOutputLines)
    assert(outputLines1 === expectedOutputLines)
  }
}
