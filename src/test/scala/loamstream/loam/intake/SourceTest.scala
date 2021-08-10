package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.TestHelpers.path
import loamstream.util.Files
import java.io.StringReader
import loamstream.loam.intake.metrics.MetricTest.MockFlipDetector
import loamstream.loam.intake.flip.FlipDetector
import loamstream.loam.intake.flip.Disposition

import scala.collection.compat._

/**
 * @author clint
 * Feb 27, 2020
 */
final class SourceTest extends FunSuite {
  
  private val rowData: Seq[Seq[String]] = {
    Seq(
      Seq("FOO", "BAR", "BAZ"),
      Seq("1",   "2",   "3"),
      Seq("9",   "8",   "7"),
      Seq("42",  "99",  "123"))
  }
  
  private val csvRows: Seq[DataRow] = Helpers.csvRows(rowData.head, rowData.tail: _*)
  
  private def rowDataToString(data: Seq[Seq[String]]): String = {
    data.map(_.mkString("\t")).mkString(System.lineSeparator)
  }
  
  private val rowDataAsString = rowDataToString(rowData)
    
  private def doCsvSourceRecordsTest(source: Source[DataRow], expectedRows: Seq[DataRow] = csvRows): Unit = {
    doCsvSourceRecordsTest(source.records.toIndexedSeq, expectedRows)
  }
  
  private def doCsvSourceRecordsTest(actualRows: Seq[DataRow], expectedRows: Seq[DataRow]): Unit = {
    actualRows.zip(expectedRows).foreach { case (actualRow, expectedRow) =>
      assert(actualRow.getFieldByName("FOO") === expectedRow.getFieldByName("FOO"))
      assert(actualRow.getFieldByName("BAR") === expectedRow.getFieldByName("BAR"))
      assert(actualRow.getFieldByName("BAZ") === expectedRow.getFieldByName("BAZ"))
    }
    
    assert(actualRows.size === expectedRows.size)
  }
  
  test("flatMap") {
    val is = Source.fromIterable(Seq(1,2,3)).flatMap { i => Source.fromIterable(Seq.fill(i)(i * 2)) }
    
    val expected = Seq(2, 4, 4, 6, 6, 6)
    
    assert(is.records.to(List) === expected)
  }
  
  test("Reading zipped input as unzipped should fail") {
    intercept[Exception] {
      Source.fromFile(
          path("src/test/resources/intake/real-input-data.tsv.gz"), 
          Source.Formats.spaceDelimitedWithHeader)
    }
  }
  
  test("Reading unzipped input as zipped should fail") {
    intercept[Exception] {
      Source.fromGzippedFile(
          path("src/test/resources/intake/real-input-data.tsv"), 
          Source.Formats.spaceDelimitedWithHeader)
    }
  }
  
  test("fromString") {
    doCsvSourceRecordsTest(Source.fromString(rowDataAsString))
  }
  
  test("fromFile") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("data.csv")
      
      Files.writeTo(file)(rowDataAsString) 
      
      doCsvSourceRecordsTest(Source.fromFile(file))
    }
  }
  
  test("fromReader") {
    doCsvSourceRecordsTest(Source.fromReader(new StringReader(rowDataAsString)))
  }
  
  test("fromCommandLine") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("data.csv")
      
      Files.writeTo(file)(rowDataAsString) 
      
      doCsvSourceRecordsTest(Source.fromCommandLine(s"cat $file", workDir = workDir))
    }
  }
  
  test("take") {
    val reader = new StringReader(rowDataToString(rowData))
    
    val source = Source.fromReader(reader).take(2)
    
    val truncatedRowData = rowData.head +: rowData.tail.take(2)
    
    doCsvSourceRecordsTest(source, Helpers.csvRows(truncatedRowData.head, truncatedRowData.tail: _*))
  }
  
  test("filter") {
    val reader = new StringReader(rowDataToString(rowData))
    
    val bar = ColumnName("BAR")
    
    val source = Source.fromReader(reader).filter(bar.asInt > 10)
    
    val expectedRows = Helpers.csvRows(
        Seq("FOO", "BAR", "BAZ"),
        Seq("42",  "99",  "123"))
        
    doCsvSourceRecordsTest(source, expectedRows)
  }
  
  test("filterNot") {
    val reader = new StringReader(rowDataToString(rowData))
    
    val bar = ColumnName("BAR")
    
    val source = Source.fromReader(reader).filterNot(bar.asInt > 10)
    
    val expectedRows = Helpers.csvRows(
        Seq("FOO", "BAR", "BAZ"),
        Seq("1",   "2",   "3"),
        Seq("9",   "8",   "7"))
        
    doCsvSourceRecordsTest(source, expectedRows)
  }
  
  private val m0 = Variant.from("1_1_A_T")
  private val m1 = Variant.from("1_2_G_C")
  private val m2 = Variant.from("2_1_T_C")
  private val m3 = Variant.from("2_2_C_G")
}
