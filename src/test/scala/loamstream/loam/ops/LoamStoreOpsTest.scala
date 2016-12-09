package loamstream.loam.ops

import java.nio.file.{Path, Files => JFiles}

import loamstream.compiler.LoamEngine
import loamstream.loam.LoamScript
import loamstream.model.jobs.JobState
import loamstream.util.Files
import loamstream.util.code.SourceUtils.Implicits.AnyToStringLiteral
import org.scalatest.FunSuite

import scala.io.{Codec, Source}


/** Test Loam store ops */
final class LoamStoreOpsTest extends FunSuite {
  private def assertFileHasNLines(path: Path, nLines: Int): Unit = {
    assert(JFiles.exists(path), s"File $path does not exist.")
    assert(Files.countLines(path) === nLines)
  }

  private def runAndAssertRunsFine(script: LoamScript, nStores: Int, nTools: Int, nJobs: Int): Unit = {
    val engine = LoamEngine.default()
    val result = engine.run(script)
    val graph = result.compileResultOpt.get.contextOpt.get.graph
    assert(graph.stores.size === nStores)
    assert(graph.tools.size === nTools)
    assert(result.jobResultsOpt.nonEmpty, result.jobResultsOpt.message)
    val jobResults = result.jobResultsOpt.get
    assert(jobResults.size === nJobs)
    assert(jobResults.values.forall(_.isInstanceOf[JobState.SuccessState]))
  }

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

  test("LoamStore.filter(...)") {
    val dir = JFiles.createTempDirectory("LoamStoreOpsTest")
    val inFile = dir.resolve("data.bim")
    Files.writeTo(inFile)(inContent)
    val unplaced = dir.resolve("unplaced.bim")
    val chr7 = dir.resolve("chr7.bim")
    val script = LoamScript("filter",
      s"""
         |val variants = store[BIM].from(${inFile.asStringLiteral})
         |val variantsUnplaced = variants.filter(StoreFieldFilter.isUndefined(BIM.chr)).to(${unplaced.asStringLiteral})
         |val variantsOnChr7 = variants.filter(BIM.chr)(_ == 7).to(${chr7.asStringLiteral})
      """.stripMargin)
    runAndAssertRunsFine(script, 3, 2, 3)
    assertFileHasNLines(unplaced, 3)
    assertFileHasNLines(chr7, 3)
  }
  test("LoamStore.map(...)") {
    val dir = JFiles.createTempDirectory("LoamStoreOpsTest")
    val inFile = dir.resolve("data.bim")
    Files.writeTo(inFile)(inContent)
    val snps = dir.resolve("snps.txt")
    val script = LoamScript("filter",
      s"""
         |val variants = store[BIM].from(${inFile.asStringLiteral})
         |val mapper = TextStoreFieldExtractor(BIM.id)
         |val snps = variants.map(mapper).to(${snps.asStringLiteral})
      """.stripMargin)
    runAndAssertRunsFine(script, 2, 1, 1)
    assertFileHasNLines(snps, 9) // scalastyle:ignore magic.number
    val snpsContent = Files.readFrom(snps)
    val snpsContentExpected = (1 to 9).map(i => s"SNP$i").mkString(System.lineSeparator)
    assert(snpsContent === snpsContentExpected)
  }
  test("LoamStore.extract(...)") {
    val dir = JFiles.createTempDirectory("LoamStoreOpsTest")
    val inFile = dir.resolve("data.bim")
    Files.writeTo(inFile)(inContent)
    val snps = dir.resolve("snps.txt")
    val script = LoamScript("filter",
      s"""
         |val variants = store[BIM].from(${inFile.asStringLiteral})
         |val snps = variants.extract(BIM.id).to(${snps.asStringLiteral})
      """.stripMargin)
    runAndAssertRunsFine(script, 2, 1, 1)
    assertFileHasNLines(snps, 9) // scalastyle:ignore magic.number
    val snpsContent = Files.readFrom(snps)
    val snpsContentExpected = (1 to 9).map(i => s"SNP$i").mkString(System.lineSeparator)
    assert(snpsContent === snpsContentExpected)
  }
  test("LoamStore.filter(...).extract(...)") {
    val dir = JFiles.createTempDirectory("LoamStoreOpsTest")
    val inFile = dir.resolve("data.bim")
    Files.writeTo(inFile)(inContent)
    val unplacedSnps = dir.resolve("unplacedSnps.txt")
    val script = LoamScript("filterMap",
      s"""
         |val variants = store[BIM].from(${inFile.asStringLiteral})
         |val unplacedSnps =
         |  variants.filter(StoreFieldFilter.isUndefined(BIM.chr)).extract(BIM.id).to(${unplacedSnps.asStringLiteral})
      """.stripMargin)
    runAndAssertRunsFine(script, 3, 2, 2)
    assertFileHasNLines(unplacedSnps, 3) // scalastyle:ignore magic.number
    val snpsContent = Files.readFrom(unplacedSnps)
    // scalastyle:off magic.number
    val snpsContentExpected = Seq(2, 4, 7).map(i => s"SNP$i").mkString(System.lineSeparator)
    // scalastyle:on magic.number
    assert(snpsContent === snpsContentExpected)
  }
}
