package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamSyntax
import java.nio.file.Path
import java.nio.file.Files
import loamstream.model.Store
import loamstream.loam.LoamScriptContext

/**
 * @author clint
 * Nov 6, 2020
 */
final class RowFiltersTest extends FunSuite {
  private object Filters extends IntakeSyntax with RowFilters
  
  import Filters.DataRowFilters
  import Filters.AggregatorVariantRowFilters
  
  private val v0 = Variant("1_12345_a_t")
  private val v1 = Variant("2_34567_T_c")
  private val v2 = Variant("3_45678_g_T")
  
  private val metadata = AggregatorMetadata(
    bucketName = "some-bucket",
    topic = Option(UploadType.Variants),
    dataset = "asdasdasd",
    phenotype = "akjdslfhsdf",
    ancestry = Ancestry.AA,
    tech = TechType.ExChip,
    quantitative = None)
    
  private def makeRow(
      marker: Variant, 
      pvalue: Double, 
      eaf: Option[Double] = None, 
      maf: Option[Double] = None): PValueVariantRow = {
    
    PValueVariantRow(
      marker = marker,
      pvalue = pvalue,
      dataset = metadata.dataset,
      phenotype = metadata.phenotype,
      ancestry = metadata.ancestry,
      eaf = eaf,
      maf = maf,
      n = 42)
  }
  
  import Helpers.Implicits.LogFileOps
  import Helpers.withLogStore
  import Helpers.linesFrom
  
  test("noDsNorIs") {
    withLogStore { logStore =>
      import Filters.DataRowFilters
      
      val REF = ColumnName("REF")
      val ALT = ColumnName("ALT")
      
      val rows = Helpers.csvRows(
          Seq(REF.name, ALT.name, "FOO"),
          Seq("D", "T", "42"),
          Seq("G", "C", "42"),
          Seq("D", "I", "42"),
          Seq("A", "I", "42"),
          Seq("A", "T", "42"))
          
      val predicate = DataRowFilters.noDsNorIs(refColumn = REF, altColumn = ALT, logStore = logStore, append = true)
      
      val filtered = rows.filter(predicate)
      
      val actual = filtered.map(_.values.toIndexedSeq)
      
      val expected = Seq(
          Seq("G", "C", "42").map(Option(_)),
          Seq("A", "T", "42").map(Option(_)))
     
      assert(actual === expected)
      
      predicate.close()
      
      val logLines = linesFrom(logStore.path)
      
      assert(logLines.size === 3)
      
      //throw new Exception(logLines.toString)
      
      assert(logLines.containsOnce(s"(${REF.name},D),(${ALT.name},T),(FOO,42)"))
      assert(logLines.containsOnce(s"(${REF.name},D),(${ALT.name},I),(FOO,42)"))
      assert(logLines.containsOnce(s"(${REF.name},A),(${ALT.name},I),(FOO,42)"))
    }
  }
  
  test("filterRefAndAlt") {
    withLogStore { logStore =>
      val REF = ColumnName("X")
      val ALT = ColumnName("Y")
      
      val rows = Helpers.csvRows(
          Seq(REF.name, ALT.name, "FOO"),
          Seq("U", "T", "42"),
          Seq("G", "C", "42"),
          Seq("U", "V", "42"),
          Seq("A", "V", "42"),
          Seq("A", "T", "42"))
          
      val predicate = DataRowFilters.filterRefAndAlt(
                                  refColumn = REF, 
                                  altColumn = ALT,
                                  disallowed = Set("U", "V"),
                                  logStore = logStore, 
                                  append = true)
      
      val filtered = rows.filter(predicate)
      
      val actual = filtered.map(_.values.toIndexedSeq)
      
      val expected = Seq(
          Seq("G", "C", "42").map(Option(_)),
          Seq("A", "T", "42").map(Option(_)))
     
      assert(actual === expected)
      
      predicate.close()
      
      val logLines = linesFrom(logStore.path)
      
      assert(logLines.size === 3)
      assert(logLines.containsOnce(s"(${REF.name},U),(${ALT.name},T),(FOO,42)"))
      assert(logLines.containsOnce(s"(${REF.name},U),(${ALT.name},V),(FOO,42)"))
      assert(logLines.containsOnce(s"(${REF.name},A),(${ALT.name},V),(FOO,42)"))
    }
  }
  
  test("logToFile") {
    withLogStore { logStore =>
      val FOO = ColumnName("FOO")
      
      val fooIsEven = FOO.asInt.map(_ % 2 == 0)
      
      val predicate = DataRowFilters.logToFile(logStore, append = true)(fooIsEven)
      
      val rows = Helpers.csvRows(
          Seq(FOO.name, "BAR"),
          Seq("1", "42"),
          Seq("2", "42"),
          Seq("3", "42"),
          Seq("6", "42"),
          Seq("7", "42"))
          
      val filtered = rows.filter(predicate)
      
      val actual = filtered.map(_.values.map(_.get).mkString(" ")).toIndexedSeq
      
      val expected = Seq(
          "2 42",
          "6 42")
          
      assert(actual === expected)
      
      predicate.close()
      
      val logLines = linesFrom(logStore.path)
      
      assert(logLines.size === 3)
      assert(logLines.containsOnce("(FOO,1),(BAR,42)"))
      assert(logLines.containsOnce("(FOO,3),(BAR,42)"))
      assert(logLines.containsOnce("(FOO,7),(BAR,42)"))
    }
  }
  
  test("logToFile - problematic beta column") {
    withLogStore { logStore =>
      val BETA = ColumnName("ALT_EFFSIZE")
      
      val data = """|CHR POS REF ALT EAF ALT_EFFSIZE PVALUE bedId chr_hg19 pos_hg19
                    |15 90225157 G A 0.00157008 1308.44 1 15:90225157:G:A 15 90768389
                    |15 90225158 C T 0.00157008 8.44 1 15:90225158:C:T 15 90768390""".stripMargin
      
      def isValid(b: Double): Boolean = b < 10.0 && b > -10.0
                    
      val betaIsValid = BETA.asDouble.map(isValid)
      
      val predicate = DataRowFilters.logToFile(logStore, append = true)(betaIsValid)
      
      val rows = Source.fromString(data, Source.Formats.spaceDelimitedWithHeader)
          
      val filtered = rows.filter(predicate)
      
      val actual = filtered.map(_.values.map(_.get).mkString(" ")).records.toIndexedSeq
      
      val expected = Seq("15 90225158 C T 0.00157008 8.44 1 15:90225158:C:T 15 90768390")
          
      assert(actual === expected)
      
      predicate.close()
      
      val logLines = linesFrom(logStore.path)
      
      assert(logLines.size === 1)
      assert(logLines.containsOnce("15:90225157:G:A"))
    }
  }
  
  test("logToFile - problematic beta column - actual data file subset") {
    withLogStore { logStore =>
      val BETA = ColumnName("ALT_EFFSIZE")
      
      def isValid(b: Double): Boolean = b < 10.0 && b > -10.0
                    
      val betaIsValid = BETA.asDouble.map(isValid)
      
      val predicate = DataRowFilters.logToFile(logStore, append = true)(betaIsValid)
      
      import TestHelpers.path
      
      val rows = Source.fromFile(path("src/test/resources/intake/bad-line.tsv"))
          
      val filtered = rows.filter(predicate)
      
      val actual = filtered.map(_.values.mkString(" ")).records.toIndexedSeq
      
      val expected = Nil
          
      assert(actual === expected)
      
      predicate.close()
      
      val logLines = linesFrom(logStore.path)
      
      assert(logLines.size === 1)
      assert(logLines.containsOnce("(bedId,15:90225157:G:A)"))
    }
  }
  
  test("validEaf") {
    withLogStore { logStore => 
      val predicate = AggregatorVariantRowFilters.validEaf(logStore, append = true)
      
      val rows = Seq(
          makeRow(marker = Variant("1_12345_T_C"), pvalue = 0.5, eaf = Some(1.0)),
          makeRow(marker = Variant("2_12345_T_C"), pvalue = 0.5, eaf = Some(0.99)),
          makeRow(marker = Variant("3_12345_T_C"), pvalue = 0.5, eaf = Some(0.0)),
          makeRow(marker = Variant("4_12345_T_C"), pvalue = 0.5, eaf = Some(-1.0)),
          makeRow(marker = Variant("2_12345_T_C"), pvalue = 0.5, eaf = Some(0.25)),
          makeRow(marker = Variant("5_12345_T_C"), pvalue = 0.5, eaf = Some(100.0)),
          makeRow(marker = Variant("6_12345_T_C"), pvalue = 0.5, eaf = Some(-10.0)))
          
      val filtered = rows.filter(predicate)
      
      val actual = filtered.flatMap(_.eaf)
      
      val expected = Seq(0.99, 0.25)
          
      assert(actual === expected)
      
      predicate.close()
      
      val logLines = linesFrom(logStore.path)
      
      assert(logLines.size === 5)
      assert(logLines.containsOnce("(1.0)"))
      assert(logLines.containsOnce("(0.0)"))
      assert(logLines.containsOnce("(-1.0)"))
      assert(logLines.containsOnce("(100.0)"))
      assert(logLines.containsOnce("(-10.0)"))
    }
  }
  
  test("validMaf") {
    withLogStore { logStore => 
      val predicate = AggregatorVariantRowFilters.validMaf(logStore, append = true)
      
      val rows = Seq(
          makeRow(marker = Variant("1_12345_T_C"), pvalue = 0.5, maf = Some(0.51)),
          makeRow(marker = Variant("2_12345_T_C"), pvalue = 0.5, maf = Some(0.5)),
          makeRow(marker = Variant("3_12345_T_C"), pvalue = 0.5, maf = Some(0.0)),
          makeRow(marker = Variant("4_12345_T_C"), pvalue = 0.5, maf = Some(-1.0)),
          makeRow(marker = Variant("2_12345_T_C"), pvalue = 0.5, maf = Some(0.25)),
          makeRow(marker = Variant("5_12345_T_C"), pvalue = 0.5, maf = Some(100.0)),
          makeRow(marker = Variant("6_12345_T_C"), pvalue = 0.5, maf = Some(-10.0)))
          
      val filtered = rows.filter(predicate)
      
      val actual = filtered.flatMap(_.maf)
      
      val expected = Seq(0.5, 0.25)
          
      assert(actual === expected)
      
      predicate.close()
      
      val logLines = linesFrom(logStore.path)
      
      assert(logLines.size === 5)
      assert(logLines.containsOnce("(0.51)"))
      assert(logLines.containsOnce("(0.0)"))
      assert(logLines.containsOnce("(-1.0)"))
      assert(logLines.containsOnce("(100.0)"))
      assert(logLines.containsOnce("(-10.0)"))
    }
  }
  
  test("validPvalue") {
    withLogStore { logStore => 
      val predicate = AggregatorVariantRowFilters.validPValue(logStore, append = true)
      
      val rows = Seq(
          makeRow(marker = Variant("1_12345_T_C"), pvalue = 1.0),
          makeRow(marker = Variant("2_12345_T_C"), pvalue = 0.99),
          makeRow(marker = Variant("3_12345_T_C"), pvalue = 0.0),
          makeRow(marker = Variant("4_12345_T_C"), pvalue = -1.0),
          makeRow(marker = Variant("2_12345_T_C"), pvalue = 0.25),
          makeRow(marker = Variant("5_12345_T_C"), pvalue = 100.0),
          makeRow(marker = Variant("6_12345_T_C"), pvalue = -10.0))
          
      val filtered = rows.filter(predicate)
      
      val actual = filtered.map(_.pvalue)
      
      val expected = Seq(1.0, 0.99, 0.25)
          
      assert(actual === expected)
      
      predicate.close()
      
      val logLines = linesFrom(logStore.path)
      
      assert(logLines.size === 4)
      assert(logLines.containsOnce("(0.0)"))
      assert(logLines.containsOnce("(-1.0)"))
      assert(logLines.containsOnce("(100.0)"))
      assert(logLines.containsOnce("(-10.0)"))
    }
  }
}

