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
  
  import Filters.CsvRowFilters
  import Filters.DataRowFilters
  
  private val v0 = Variant("1_12345_a_t")
  private val v1 = Variant("2_34567_T_c")
  private val v2 = Variant("3_45678_g_T")
  
  import Helpers.Implicits.LogFileOps
  import Helpers.withLogStore
  import Helpers.linesFrom
  
  test("noDsNorIs") {
    withLogStore { logStore =>
      import Filters.CsvRowFilters
      
      val REF = ColumnName("REF")
      val ALT = ColumnName("ALT")
      
      val rows = Helpers.csvRows(
          Seq(REF.name, ALT.name, "FOO"),
          Seq("D", "T", "42"),
          Seq("G", "C", "42"),
          Seq("D", "I", "42"),
          Seq("A", "I", "42"),
          Seq("A", "T", "42"))
          
      val predicate = CsvRowFilters.noDsNorIs(refColumn = REF, altColumn = ALT, logStore = logStore, append = true)
      
      val filtered = rows.filter(predicate)
      
      val actual = filtered.map(_.values.toIndexedSeq)
      
      val expected = Seq(
          Seq("G", "C", "42"),
          Seq("A", "T", "42"))
     
      assert(actual === expected)
      
      predicate.close()
      
      val logLines = linesFrom(logStore.path)
      
      assert(logLines.size === 3)
      assert(logLines.containsOnce("D,T,42"))
      assert(logLines.containsOnce("D,I,42"))
      assert(logLines.containsOnce("A,I,42"))
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
          
      val predicate = CsvRowFilters.filterRefAndAlt(
                                  refColumn = REF, 
                                  altColumn = ALT,
                                  disallowed = Set("U", "V"),
                                  logStore = logStore, 
                                  append = true)
      
      val filtered = rows.filter(predicate)
      
      val actual = filtered.map(_.values.toIndexedSeq)
      
      val expected = Seq(
          Seq("G", "C", "42"),
          Seq("A", "T", "42"))
     
      assert(actual === expected)
      
      predicate.close()
      
      val logLines = linesFrom(logStore.path)
      
      assert(logLines.size === 3)
      assert(logLines.containsOnce("U,T,42"))
      assert(logLines.containsOnce("U,V,42"))
      assert(logLines.containsOnce("A,V,42"))
    }
  }
  
  test("logToFile") {
    withLogStore { logStore =>
      val FOO = ColumnName("FOO")
      
      val fooIsEven = FOO.asInt.map(_ % 2 == 0)
      
      val predicate = CsvRowFilters.logToFile(logStore, append = true)(fooIsEven)
      
      val rows = Helpers.csvRows(
          Seq(FOO.name, "BAR"),
          Seq("1", "42"),
          Seq("2", "42"),
          Seq("3", "42"),
          Seq("6", "42"),
          Seq("7", "42"))
          
      val filtered = rows.filter(predicate)
      
      val actual = filtered.map(_.values.mkString(" ")).toIndexedSeq
      
      val expected = Seq(
          "2 42",
          "6 42")
          
      assert(actual === expected)
      
      predicate.close()
      
      val logLines = linesFrom(logStore.path)
      
      assert(logLines.size === 3)
      assert(logLines.containsOnce("1,42"))
      assert(logLines.containsOnce("3,42"))
      assert(logLines.containsOnce("7,42"))
    }
  }
  
  test("validEaf") {
    withLogStore { logStore => 
      val predicate = DataRowFilters.validEaf(logStore, append = true)
      
      val rows = Seq(
          AggregatorVariantRow(marker = Variant("1_12345_T_C"), pvalue = 0.5, eaf = Some(1.0)),
          AggregatorVariantRow(marker = Variant("2_12345_T_C"), pvalue = 0.5, eaf = Some(0.99)),
          AggregatorVariantRow(marker = Variant("3_12345_T_C"), pvalue = 0.5, eaf = Some(0.0)),
          AggregatorVariantRow(marker = Variant("4_12345_T_C"), pvalue = 0.5, eaf = Some(-1.0)),
          AggregatorVariantRow(marker = Variant("2_12345_T_C"), pvalue = 0.5, eaf = Some(0.25)),
          AggregatorVariantRow(marker = Variant("5_12345_T_C"), pvalue = 0.5, eaf = Some(100.0)),
          AggregatorVariantRow(marker = Variant("6_12345_T_C"), pvalue = 0.5, eaf = Some(-10.0)))
          
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
      val predicate = DataRowFilters.validMaf(logStore, append = true)
      
      val rows = Seq(
          AggregatorVariantRow(marker = Variant("1_12345_T_C"), pvalue = 0.5, maf = Some(0.51)),
          AggregatorVariantRow(marker = Variant("2_12345_T_C"), pvalue = 0.5, maf = Some(0.5)),
          AggregatorVariantRow(marker = Variant("3_12345_T_C"), pvalue = 0.5, maf = Some(0.0)),
          AggregatorVariantRow(marker = Variant("4_12345_T_C"), pvalue = 0.5, maf = Some(-1.0)),
          AggregatorVariantRow(marker = Variant("2_12345_T_C"), pvalue = 0.5, maf = Some(0.25)),
          AggregatorVariantRow(marker = Variant("5_12345_T_C"), pvalue = 0.5, maf = Some(100.0)),
          AggregatorVariantRow(marker = Variant("6_12345_T_C"), pvalue = 0.5, maf = Some(-10.0)))
          
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
      val predicate = DataRowFilters.validPValue(logStore, append = true)
      
      val rows = Seq(
          AggregatorVariantRow(marker = Variant("1_12345_T_C"), pvalue = 1.0),
          AggregatorVariantRow(marker = Variant("2_12345_T_C"), pvalue = 0.99),
          AggregatorVariantRow(marker = Variant("3_12345_T_C"), pvalue = 0.0),
          AggregatorVariantRow(marker = Variant("4_12345_T_C"), pvalue = -1.0),
          AggregatorVariantRow(marker = Variant("2_12345_T_C"), pvalue = 0.25),
          AggregatorVariantRow(marker = Variant("5_12345_T_C"), pvalue = 100.0),
          AggregatorVariantRow(marker = Variant("6_12345_T_C"), pvalue = -10.0))
          
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

