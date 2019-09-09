package loamstream.googlecloud

import java.nio.file.Files.exists

import scala.io.Source

import org.scalatest.FunSuite

import loamstream.IntegrationTestHelpers
import loamstream.apps.Main
import loamstream.util.CanBeClosed
import loamstream.util.{ Files => LFiles }
import loamstream.util.Paths.Implicits.PathHelpers
import loamstream.IntegrationTestHelpers.path
import loamstream.conf.LoamConfig
import com.typesafe.config.ConfigFactory


/**
 * @author clint
 * Jun 11, 2019
 */
final class HailAtGoogleTest extends FunSuite {
  
  
  test("Copy a file to Google, run a simple Hail job with it, and copy the result to the local FS.") {
    val workDir = IntegrationTestHelpers.getWorkDirUnderTarget().toAbsolutePath
    
    val testConfContents = s"""|googleCloudUri = "gs://loamstream/dev/integration_tests/hail-at-google"
                               |workDir = "${workDir}"
                               |""".stripMargin.trim
                               
    val testConf = workDir / "test.conf"
    
    LFiles.writeTo(testConf)(testConfContents)
    
    System.setProperty("dataConfig", testConf.toString)

    val expectedOutput = workDir / "output.tsv"
    
    assert(exists(expectedOutput) === false)
    
    val resourceDir = path("src/it/resources/hail-at-google")
    
    val loamstreamDotConf = resourceDir / "loamstream.conf"
    
    //Sanity check
    val googleConfig = LoamConfig.fromConfig(ConfigFactory.parseFile(loamstreamDotConf.toFile)).get.googleConfig.get
    
    assert(googleConfig.defaultClusterConfig !== ClusterConfig.default)
    
    //Now run for real
    Main.main(Array(
        "--conf", loamstreamDotConf.toString,
        "--loams", (resourceDir / "test.loam").toString))
    
    val outputLines = CanBeClosed.enclosed(Source.fromFile(expectedOutput.toFile)) {
      _.getLines.map(_.trim).toIndexedSeq
    }
       
    assert(outputLines(0) === "AFR\tAMR\tEUR\tEAS\tSAS\tTOTAL")
    assert(outputLines(1) === "347\t901\t10045\t324\t75\t11692")
  }
}
