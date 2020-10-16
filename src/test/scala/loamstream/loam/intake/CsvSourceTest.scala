package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.util.Files
import java.io.StringReader

/**
 * @author clint
 * Feb 27, 2020
 */
final class CsvSourceTest extends FunSuite {
  
  private val rowData: Seq[Seq[String]] = {
    Seq(
      Seq("FOO", "BAR", "BAZ"),
      Seq("1",   "2",   "3"),
      Seq("9",   "8",   "7"),
      Seq("42",  "99",  "123"))
  }
  
  private val csvRows: Seq[CsvRow] = Helpers.csvRows(rowData.head, rowData.tail: _*)
  
  private def rowDataToString(data: Seq[Seq[String]]): String = {
    data.map(_.mkString("\t")).mkString(System.lineSeparator)
  }
  
  private val rowDataAsString = rowDataToString(rowData)
    
  private def doCsvSourceRecordsTest(source: Source[CsvRow], expectedRows: Seq[CsvRow] = csvRows): Unit = {
    val actualRows = source.records.toIndexedSeq
    
    actualRows.zip(expectedRows).foreach { case (actualRow, expectedRow) =>
      assert(actualRow.getFieldByName("FOO") === expectedRow.getFieldByName("FOO"))
      assert(actualRow.getFieldByName("BAR") === expectedRow.getFieldByName("BAR"))
      assert(actualRow.getFieldByName("BAZ") === expectedRow.getFieldByName("BAZ"))
    }
    
    assert(actualRows.size === expectedRows.size)
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
}
