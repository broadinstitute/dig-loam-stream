package loamstream.loam.ops

import java.nio.file.{Path, Files => JFiles}

import loamstream.compiler.LoamEngine
import loamstream.loam.LoamScript
import loamstream.model.jobs.JobState
import loamstream.util.Files
import loamstream.util.code.SourceUtils.Implicits.AnyToStringLiteral
import org.scalatest.FunSuite


/** Test Loam store ops */
final class LoamStoreOpsTest extends FunSuite {
  private def assertFileExists(path: Path): Unit = assert(JFiles.exists(path), s"File $path does not exist.")

  test("LoamStore.filter()") {
    val dir = JFiles.createTempDirectory("LoamStoreOpsTest")
    val inFile = dir.resolve("data.bim")
    val inContent =
      """
        |3 SNP1 0 123 A G
        | SNP2 0 456 T C
        |7 SNP3 0 789 A T
        | SNP4 0 123 A G
        |3 SNP5 0 456 T C
        |7 SNP6 0 789 A T
        | SNP7 0 123 A G
        |3 SNP8 0 456 T C
        |7 SNP9 0 789 A T
      """.stripMargin.trim
    Files.writeTo(inFile)(inContent)
    val unplaced = dir.resolve("unplaced.bim")
    val chr7 = dir.resolve("chr7.bim")
    val script = LoamScript("filter",
      s"""
         |val variants = store[BIM].from(${inFile.asStringLiteral})
         |val variantsUnplaced = variants.filter(StoreFieldFilter.isUndefined(BIM.chr)).to(${unplaced.asStringLiteral})
         |val variantsOnChr7 = variants.filter(BIM.chr)(_ == 7).to(${chr7.asStringLiteral})
      """.stripMargin)
    val engine = LoamEngine.default()
    val result = engine.run(script)
    val graph = result.compileResultOpt.get.contextOpt.get.graph
    assert(graph.stores.size === 3)
    assert(graph.tools.size === 2)
    assert(result.jobResultsOpt.nonEmpty, result.jobResultsOpt.message)
    val jobResults = result.jobResultsOpt.get
    assert(jobResults.size === 3)
    assert(jobResults.values.forall(_.isInstanceOf[JobState.SuccessState]))
    assertFileExists(unplaced)
    assert(Files.countLines(unplaced) === 3)
    assertFileExists(chr7)
    assert(Files.countLines(chr7) === 3)
  }
}
