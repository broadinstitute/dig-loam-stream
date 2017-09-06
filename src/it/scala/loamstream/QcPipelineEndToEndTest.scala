package loamstream

import java.nio.file.Path

import org.scalatest.FunSuite

import loamstream.util.ExitCodes
import loamstream.util.Files

/**
 * @author clint
 * Apr 21, 2017
 */
final class QcPipelineEndToEndTest extends FunSuite {
  import IntegrationTestHelpers.{ path, withLoudStackTraces }
  
  private val referenceDir = path("/humgen/diabetes/users/dig/loamstream/ci/test-data/qc/camp/results")
  private val outputDir = path("./qc")

  test("Run the QC pipeline end-to-end on real data") {
    withLoudStackTraces {
      run()
    }

    //NB: Deterministic outputs from the penultimate Klustaskwik jobs
    //TODO: Better or different set of outputs to compare
    val filesToCheck: Seq[Path] = Seq(
      path("CAMP.sampleqc.stats.adj.callRate_res.clu.1"),
      path("CAMP.sampleqc.stats.adj.callRate_res.fet.1"),
      path("CAMP.sampleqc.stats.adj.cluster.clu.1"),
      path("CAMP.sampleqc.stats.adj.cluster.fet.1"),
      path("CAMP.sampleqc.stats.adj.cluster.outliers"),
      path("CAMP.sampleqc.stats.adj.cluster.xtabs"),
      path("CAMP.sampleqc.stats.adj.hetHigh_res.clu.1"),
      path("CAMP.sampleqc.stats.adj.hetHigh_res.fet.1"),
      path("CAMP.sampleqc.stats.adj.hetLow_res.clu.1"),
      path("CAMP.sampleqc.stats.adj.hetLow_res.fet.1"),
      path("CAMP.sampleqc.stats.adj.het_res.clu.1"),
      path("CAMP.sampleqc.stats.adj.het_res.fet.1"),
      path("CAMP.sampleqc.stats.adj.ind.discreteness"),
      path("CAMP.sampleqc.stats.adj.nCalled_res.clu.1"),
      path("CAMP.sampleqc.stats.adj.nCalled_res.fet.1"),
      path("CAMP.sampleqc.stats.adj.nHet_res.clu.1"),
      path("CAMP.sampleqc.stats.adj.nHet_res.fet.1"),
      path("CAMP.sampleqc.stats.adj.nHomVar_res.clu.1"),
      path("CAMP.sampleqc.stats.adj.nHomVar_res.fet.1"),
      path("CAMP.sampleqc.stats.adj.nNonRef_res.clu.1"),
      path("CAMP.sampleqc.stats.adj.nNonRef_res.fet.1"),
      path("CAMP.sampleqc.stats.adj.pca.loadings.tsv"),
      path("CAMP.sampleqc.stats.adj.pca.scores.tsv"),
      path("CAMP.sampleqc.stats.adj.rHetHomVar_res.clu.1"),
      path("CAMP.sampleqc.stats.adj.rHetHomVar_res.fet.1"),
      path("CAMP.sampleqc.stats.adj.rTiTv_res.clu.1"),
      path("CAMP.sampleqc.stats.adj.rTiTv_res.fet.1"),
      path("CAMP.sampleqc.stats.adj.tsv"))

    val pairsToCompare: Seq[(Path, Path)] = filesToCheck.map(p => (referenceDir.resolve(p), outputDir.resolve(p)))

    pairsToCompare.foreach(diff.tupled)
  }

  private def run(): Unit = {
    Files.createDirsIfNecessary(outputDir)

    Files.createDirsIfNecessary(path("./uger"))

    val args: Array[String] = {
      Array(
          "--conf",
          "pipeline/conf/loamstream.conf",
          "pipeline/loam/qc.loam",
          "pipeline/loam/config.loam",
          "pipeline/loam/binaries.loam",
          "pipeline/loam/scripts.loam",
          "pipeline/loam/cloud_helpers.loam",
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
