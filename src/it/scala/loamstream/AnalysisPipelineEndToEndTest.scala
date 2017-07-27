package loamstream

import java.nio.file.Path

import loamstream.util.Files
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 7/24/17
 */
final class AnalysisPipelineEndToEndTest extends FunSuite {
  private val outputDir = TestHelpers.path("./analysis")

  test("Run the analysis pipeline end-to-end on CAMP data") {
    try {
      run()
    } catch {
      //NB: SBT drastically truncates stack traces. so print them manually to get more info.  
      //This workaround is lame, but gives us a chance at debugging failures.
      case e: Throwable => e.printStackTrace() ; throw e
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

    Files.createDirsIfNecessary(TestHelpers.path("./uger"))

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
