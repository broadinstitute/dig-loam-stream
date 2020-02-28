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
    
  private def doCsvSourceRecordsTest(source: CsvSource, expectedRows: Seq[CsvRow] = csvRows): Unit = {
    val actualRows = source.records.toIndexedSeq
    
    actualRows.zip(expectedRows).foreach { case (actualRow, expectedRow) =>
      assert(actualRow.getFieldByName("FOO") === expectedRow.getFieldByName("FOO"))
      assert(actualRow.getFieldByName("BAR") === expectedRow.getFieldByName("BAR"))
      assert(actualRow.getFieldByName("BAZ") === expectedRow.getFieldByName("BAZ"))
    }
    
    assert(actualRows.size === expectedRows.size)
  }
  
  test("fromFile") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("data.csv")
      
      Files.writeTo(file)(rowDataAsString) 
      
      doCsvSourceRecordsTest(CsvSource.fromFile(file))
    }
  }
  
  test("fromReader") {
    doCsvSourceRecordsTest(CsvSource.fromReader(new StringReader(rowDataAsString)))
  }
  
  test("fromCommandLine") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("data.csv")
      
      Files.writeTo(file)(rowDataAsString) 
      
      doCsvSourceRecordsTest(CsvSource.fromCommandLine(s"cat $file", workDir = workDir))
    }
  }
  
  test("take") {
    val reader = new StringReader(rowDataToString(rowData))
    
    val source = CsvSource.fromReader(reader).take(2)
    
    val truncatedRowData = rowData.head +: rowData.tail.take(2)
    
    doCsvSourceRecordsTest(source, Helpers.csvRows(truncatedRowData.head, truncatedRowData.tail: _*))
  }
  
  test("filter") {
    val reader = new StringReader(rowDataToString(rowData))
    
    val bar = ColumnName("BAR")
    
    val source = CsvSource.fromReader(reader).filter(bar.asInt > 10)
    
    val expectedRows = Helpers.csvRows(
        Seq("FOO", "BAR", "BAZ"),
        Seq("42",  "99",  "123"))
        
    doCsvSourceRecordsTest(source, expectedRows)
  }
  
  test("filterNot") {
    val reader = new StringReader(rowDataToString(rowData))
    
    val bar = ColumnName("BAR")
    
    val source = CsvSource.fromReader(reader).filterNot(bar.asInt > 10)
    
    val expectedRows = Helpers.csvRows(
        Seq("FOO", "BAR", "BAZ"),
        Seq("1",   "2",   "3"),
        Seq("9",   "8",   "7"))
        
    doCsvSourceRecordsTest(source, expectedRows)
  }
  
  /**
   * final def producing(columnDef: UnsourcedColumnDef): SourcedColumnDef = addSourceTo(columnDef)
  
  final def producing(columnDefs: Seq[UnsourcedColumnDef]): Seq[SourcedColumnDef] = columnDefs.map(addSourceTo)
   */
  
  test("producing") {
    //dummy source, doesn't need to produce anything
    val source = CsvSource.fromReader(new StringReader(""))
    
    val cd0 = ColumnDef(ColumnName("FOO"))
    val cd1 = ColumnDef(ColumnName("BAR"))
    val cd2 = ColumnDef(ColumnName("BAZ"))
    
    assert(source.producing(cd0) === SourcedColumnDef(cd0, source))
    
    val expected = Seq(cd0, cd1, cd2).map(SourcedColumnDef(_, source))
    
    assert(source.producing(Seq(cd0, cd1, cd2)) === expected)
  }
}
