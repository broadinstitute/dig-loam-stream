package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.loam.intake.flip.Disposition

/**
 * @author clint
 * Nov 19, 2020
 */
object CsvRowTest {
  private[intake] trait RowHelpers { self: FunSuite =>
    def oneRow: DataRow = {
      val csvData = s"""|X Y Z
                        |1 hello blah_123""".stripMargin
      
      val rows = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)
          
      val rowsList = rows.records.toList
      
      assert(rowsList.size === 1)
      
      rowsList.head
    }
  }
}

final class CsvRowTest extends FunSuite with CsvRowTest.RowHelpers {
  test("CommonsCsvRow") {
    val firstRow = oneRow
    
    assert(firstRow.isSkipped === false)
    assert(firstRow.notSkipped === true)
    assert(firstRow.size === 3)
    assert(firstRow.recordNumber === 1)
    
    assert(firstRow.getFieldByName("X") === "1")
    assert(firstRow.getFieldByName("Y") === "hello")
    assert(firstRow.getFieldByName("Z") === "blah_123")
    
    assert(firstRow.getFieldByIndex(0) === "1")
    assert(firstRow.getFieldByIndex(1) === "hello")
    assert(firstRow.getFieldByIndex(2) === "blah_123")
    
    assert(firstRow.values.toList === Seq("1", "hello", "blah_123"))
    
    val skipped = firstRow.skip
    
    assert(firstRow ne skipped)
    
    assert(firstRow.isSkipped === false)
    assert(firstRow.notSkipped === true)
    
    assert(skipped.isSkipped === true)
    assert(skipped.notSkipped === false)
    
    assert(firstRow.values.toList === Seq("1", "hello", "blah_123"))
    
    assert(firstRow.values.toList === skipped.values.toList)
  }
  
}
