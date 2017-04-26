package loamstream

import org.scalatest.FunSuite
import java.nio.file.Path
import loamstream.util.ExitCodes
import java.nio.file.Paths
import java.nio.file.{Files => JFiles}
import loamstream.util.Files

/**
 * @author clint
 * Apr 21, 2017
 */
final class QcPipelineEndToEndTest extends FunSuite {
  import JFiles.exists

  private val referenceDir = path("/humgen/diabetes/users/dig/loamstream/ci/test-data/qc/camp/results")
  private val outputDir = path("./qc")

  test("Run the QC pipeline end-to-end on real data") {
    try {
      run()
    } catch {
      //NB: SBT drastically truncates stack traces. so print them manually to get more info.  
      //This workaround is lame, but gives us a chance at debugging failures.
      case e: Throwable => e.printStackTrace() ; throw e
    }

    //NB: Deterministic outputs from the penultimate Klustaskwik jobs
    //TODO: Better or different set of outputs to compare
    val filesToCheck: Seq[Path] = Seq(
      path("CAMP.sampleqc.stats.adj.1.fet.1"),
      path("CAMP.sampleqc.stats.adj.10.fet.1"),
      path("CAMP.sampleqc.stats.adj.2.fet.1"),
      path("CAMP.sampleqc.stats.adj.3.fet.1"),
      path("CAMP.sampleqc.stats.adj.4.fet.1"),
      path("CAMP.sampleqc.stats.adj.5.fet.1"),
      path("CAMP.sampleqc.stats.adj.6.fet.1"),
      path("CAMP.sampleqc.stats.adj.7.fet.1"),
      path("CAMP.sampleqc.stats.adj.8.fet.1"),
      path("CAMP.sampleqc.stats.adj.9.fet.1"),
      path("CAMP.sampleqc.stats.adj.1.clu.1"),
      path("CAMP.sampleqc.stats.adj.1.temp.clu.1"),
      path("CAMP.sampleqc.stats.adj.10.clu.1"),
      path("CAMP.sampleqc.stats.adj.10.temp.clu.1"),
      path("CAMP.sampleqc.stats.adj.2.clu.1"),
      path("CAMP.sampleqc.stats.adj.2.temp.clu.1"),
      path("CAMP.sampleqc.stats.adj.3.clu.1"),
      path("CAMP.sampleqc.stats.adj.3.temp.clu.1"),
      path("CAMP.sampleqc.stats.adj.4.clu.1"),
      path("CAMP.sampleqc.stats.adj.4.temp.clu.1"),
      path("CAMP.sampleqc.stats.adj.5.clu.1"),
      path("CAMP.sampleqc.stats.adj.5.temp.clu.1"),
      path("CAMP.sampleqc.stats.adj.6.clu.1"),
      path("CAMP.sampleqc.stats.adj.6.temp.clu.1"),
      path("CAMP.sampleqc.stats.adj.7.clu.1"),
      path("CAMP.sampleqc.stats.adj.7.temp.clu.1"),
      path("CAMP.sampleqc.stats.adj.8.clu.1"),
      path("CAMP.sampleqc.stats.adj.8.temp.clu.1"),
      path("CAMP.sampleqc.stats.adj.9.clu.1"),
      path("CAMP.sampleqc.stats.adj.9.temp.clu.1"),
      path("CAMP.sampleqc.stats.adj.temp.clu.1"))

    val pairsToCompare: Seq[(Path, Path)] = filesToCheck.map(p => (referenceDir.resolve(p), outputDir.resolve(p)))

    pairsToCompare.foreach(diff.tupled)
  }

  private def path(s: String): Path = Paths.get(s)

  private def run(): Unit = {
    Files.createDirsIfNecessary(outputDir)
    
    Files.createDirsIfNecessary(path("./uger"))

    val args: Array[String] = {
      Array(
          "--conf", 
          "pipeline/loam/qc.conf",
          "pipeline/loam/binaries.loam",
          "pipeline/loam/cloud_helpers.loam",
          "pipeline/loam/input.loam",
          //NB: This is the CI qc_params.loam, that will make us run over fewer chromosomes.
          "pipeline/loam/ci/qc_params.loam",
          "pipeline/loam/qc.loam",
          "pipeline/loam/scripts.loam",
          "pipeline/loam/store_helpers.loam")
    }
    
    loamstream.apps.Main.main(args)
  }

  private val diff: (Path, Path) => Unit = { (a, b) =>
    import scala.sys.process._

    //NB: Shell out to diff
    val exitCode = s"diff -q $a $b".!

    assert(ExitCodes.isSuccess(exitCode), s"$a and $b differ, or $b doesn't exist")
  }
}
