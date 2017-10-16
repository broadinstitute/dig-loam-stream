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
      path("camp.gwas1.clean.bed"),
      path("camp.gwas1.clean.bim"),
      path("camp.gwas1.clean.fam"),
      path("camp.gwas1.clean.vcf.bgz"),
      path("camp.gwas1.clean.vcf.bgz.tbi"),
      path("camp.gwas1.final.sample.exclusions"),
      path("camp.gwas1.final.variant.exclusions"),
      path("camp.gwas1.sampleqc.outliers.tsv"),
      path("camp.gwas1.sampleqc.sexcheck.problems.tsv"),
      path("camp.gwas1.sampleqc.stats.tsv"),
      path("camp.gwas1.variantqc.stats.tsv"),
      path("camp.gwas1.ancestry.inferred.tsv"))

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
