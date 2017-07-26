package loamstream

import java.nio.file.{Path, Paths}

import loamstream.util.Files
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 7/24/17
 */
final class AnalysisPipelineEndToEndTest extends FunSuite {

  private val referenceDir = path("/humgen/diabetes/users/dig/loamstream/ci/test-data/analysis/camp/results")
  private val outputDir = path("./analysis")

  test("Run the analysis pipeline end-to-end on CAMP data") {
    try {
      run()
    } catch {
      //NB: SBT drastically truncates stack traces. so print them manually to get more info.  
      //This workaround is lame, but gives us a chance at debugging failures.
      case e: Throwable => e.printStackTrace() ; throw e
    }

    val regionsFileNameRegex = "CAMP.GLU_FAST_RIN.chr*.OK"
    val expectedNumberOfRegionsFiles = 22

    assert(countFiles(regionsFileNameRegex, outputDir) === expectedNumberOfRegionsFiles)

    val epactsOkFileNameRegex = "chr*.regions"
    val expectedNumberOfEpactsOkFiles = 93

    assert(countFiles(epactsOkFileNameRegex, outputDir) === expectedNumberOfEpactsOkFiles)
  }

  private def path(s: String): Path = Paths.get(s)

  private def run(): Unit = {
    Files.createDirsIfNecessary(outputDir)

    Files.createDirsIfNecessary(path("./uger"))

    // Turn off job skipping to prevent race conditions with database generation,
    // and also because CI builds are created from scratch each run anyway
    val args: Array[String] = {
      Array(
          "--run-everything",
          "--conf",
          "pipeline/loam/qc.conf",
          "pipeline/loam/analysis.loam")
    }

    loamstream.apps.Main.main(args)
  }

  def countFiles(regex: String, dir: Path): Int = {
    import scala.sys.process._

    (s"ls $dir/$regex" #| "wc -l").!!.trim.toInt
  }
}
