package loamstream.loam.ops

import java.nio.file.{Path, Files => JFiles}

import loamstream.compiler.LoamEngine
import loamstream.loam.LoamScript
import loamstream.util.Files
import loamstream.util.code.SourceUtils.Implicits.AnyToStringLiteral
import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.model.jobs.{JobResult, JobStatus, NoOpJob}
import loamstream.loam.LoamGraph
import loamstream.compiler.LoamPredef
import loamstream.loam.LoamCmdTool
import loamstream.loam.ops.filters.StoreFieldFilter
import loamstream.loam.ops.mappers.TextStoreFieldExtractor


/** Test Loam store ops */
final class LoamStoreOpsTest extends FunSuite {
  private def assertFileHasNLines(path: Path, nLines: Int): Unit = {
    assert(JFiles.exists(path), s"File $path does not exist.")
    assert(Files.countLines(path) === nLines)
  }

  private def runAndAssertRunsFine(graph: LoamGraph, nStores: Int, nTools: Int, nJobs: Int): Unit = {
    val jobExecutions = TestHelpers.run(graph)
    
    assert(graph.stores.size === nStores)
    assert(graph.tools.size === nTools)
    
    assert(jobExecutions.size === nJobs)
    val jobStatuses = jobExecutions.values.map(_.status)
    assert(jobStatuses.forall(_ == JobStatus.Succeeded))
    val nonNoOpJobResults = jobExecutions.filterKeys(!_.isInstanceOf[NoOpJob]).values.map(_.result)
    assert(nonNoOpJobResults.forall(_.get == JobResult.Success))
  }

  private val inContent = {
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
  }

  test("LoamStore.filter(...)") {
    val dir = JFiles.createTempDirectory("LoamStoreOpsTest")
    val inFile = dir.resolve("data.bim")
    Files.writeTo(inFile)(inContent)
    val unplaced = dir.resolve("unplaced.bim")
    val chr7 = dir.resolve("chr7.bim")
    val graph = TestHelpers.makeGraph { implicit sc =>
      import LoamPredef._
      import LoamCmdTool._
      import StoreType._
      
      val variants = store[BIM].at(inFile).asInput
      val variantsUnplaced = variants.filter(StoreFieldFilter.isUndefined(BIM.chr)).at(unplaced)
      val variantsOnChr7 = variants.filter(BIM.chr)(_ == 7).at(chr7)
    }
    runAndAssertRunsFine(graph, 3, 2, 3)
    assertFileHasNLines(unplaced, 3)
    assertFileHasNLines(chr7, 3)
  }
  
  test("LoamStore.map(...)") {
    val dir = JFiles.createTempDirectory("LoamStoreOpsTest")
    val inFile = dir.resolve("data.bim")
    Files.writeTo(inFile)(inContent)
    val snps: Path = dir.resolve("snps.txt")
    
    val graph = TestHelpers.makeGraph { implicit sc =>
      import LoamPredef._
      import LoamCmdTool._
      import StoreType._
      
      val variants = store[BIM].at(inFile).asInput
      val mapper = TextStoreFieldExtractor(BIM.id)
      val mapped = variants.map(mapper).at(snps)
    }
    
    runAndAssertRunsFine(graph, 2, 1, 1)
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
    val graph = TestHelpers.makeGraph { implicit sc =>
      import LoamPredef._
      import LoamCmdTool._
      import StoreType._

      val variants = store[BIM].at(inFile).asInput
      val extracted = variants.extract(BIM.id).at(snps)
    }
      
    runAndAssertRunsFine(graph, 2, 1, 1)
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
    
    val graph = TestHelpers.makeGraph { implicit sc =>
      import LoamPredef._
      import LoamCmdTool._
      import StoreType._

      val variants = store[BIM].at(inFile).asInput
      val filtered = variants.filter(StoreFieldFilter.isUndefined(BIM.chr)).extract(BIM.id).at(unplacedSnps)
    }
    
    runAndAssertRunsFine(graph, 3, 2, 2)
    assertFileHasNLines(unplacedSnps, 3) // scalastyle:ignore magic.number
    val snpsContent = Files.readFrom(unplacedSnps)
    // scalastyle:off magic.number
    val snpsContentExpected = Seq(2, 4, 7).map(i => s"SNP$i").mkString(System.lineSeparator)
    // scalastyle:on magic.number
    assert(snpsContent === snpsContentExpected)
  }
}
