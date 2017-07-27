package loamstream

import java.nio.file.Path

import org.scalatest.FunSuite

import loamstream.util.Files

/**
 * @author kyuksel
 *         date: 7/24/17
 */
final class AnalysisPipelineEndToEndTest extends FunSuite {
  import IntegrationTestHelpers.{ path, withLoudStackTraces }
  
  private val outputDir = path("./analysis")

  test("Run the analysis pipeline end-to-end on CAMP data") {
    withLoudStackTraces {
      run()
    }

    val regionsFileNameRegex = "chr.*regions"
    val expectedNumberOfRegionsFiles = 22

    assert(countFiles(regionsFileNameRegex, outputDir) === expectedNumberOfRegionsFiles)

    val epactsOkFileNameRegex = "CAMP.GLU_FAST_RIN.chr.*epacts.OK"
    val expectedNumberOfEpactsOkFiles = 93

    assert(countFiles(epactsOkFileNameRegex, outputDir) === expectedNumberOfEpactsOkFiles)
  }

  private def run(): Unit = {
    Files.createDirsIfNecessary(outputDir)

    Files.createDirsIfNecessary(path("./uger"))

    val args: Array[String] = {
      Array(
          "--conf",
          "pipeline/loam/qc.conf",
          "pipeline/loam/analysis.loam")
    }

    loamstream.apps.Main.main(args)
  }

  private def countFiles(regex: String, dir: Path): Int = Files.listFiles(dir, regex).size
}
