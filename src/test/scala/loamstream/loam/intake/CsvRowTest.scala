package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.loam.intake.flip.Disposition

/**
 * @author clint
 * Nov 19, 2020
 */
final class CsvRowTest extends FunSuite {
  private def oneRow: CsvRow = {
    val csvData = s"""|X Y Z
                      |1 hello blah_123""".stripMargin
    
    val rows = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)
        
    val rowsList = rows.records.toList
    
    assert(rowsList.size === 1)
    
    rowsList.head
  }
  
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
  
  test("Tagged") {
    val delegate = oneRow
    
    def doTest(disp: Disposition): Unit = {
      val marker = Variant.from("1_12345_A_T")
      
      val tagged = CsvRow.Tagged(delegate, marker, marker, disp)
      
      assert(tagged.isSkipped === false)
      assert(tagged.notSkipped === true)
      
      assert(tagged.delegate eq delegate)
      
      assert(tagged.isFlipped === disp.isFlipped)
      assert(tagged.notFlipped === disp.notFlipped)
      assert(tagged.isSameStrand === disp.isSameStrand)
      assert(tagged.isComplementStrand === disp.isComplementStrand)
      
      assert(tagged.size === delegate.size)
      
      assert(tagged.values.toList === delegate.values.toList)
      
      assert(tagged.recordNumber === delegate.recordNumber)
      
      assert(tagged.getFieldByIndex(0) === "1")
      assert(tagged.getFieldByIndex(1) === "hello")
      assert(tagged.getFieldByIndex(2) === "blah_123")
      
      val skipped = tagged.skip
      
      assert(skipped ne tagged)
      
      assert(skipped.isSkipped === true)
      assert(skipped.notSkipped === false)
      
      assert(skipped.values.toList === tagged.values.toList)
    }
    
    doTest(Disposition.FlippedComplementStrand)
    doTest(Disposition.FlippedSameStrand)
    doTest(Disposition.NotFlippedComplementStrand)
    doTest(Disposition.NotFlippedSameStrand)
  }
  
  test("Transformed") {
    val delegate = oneRow
    
    val marker = Variant.from("1_12345_A_T")
      
    val disp = Disposition.FlippedSameStrand
    
    val tagged = CsvRow.Tagged(delegate, marker, marker, disp)
    
    val dataRow = DataRow(marker, 42.0)
    
    val transformed = CsvRow.Transformed(tagged, dataRow)
    
    assert(transformed.isSkipped === false)
    
    assert(transformed.skip === CsvRow.Skipped(tagged, Some(dataRow)))
    
    def incPvalue(dr: DataRow): DataRow = dr.copy(pvalue = dr.pvalue + 1.0) 
    
    val furtherTransformed = transformed.transform(incPvalue)
    
    assert(furtherTransformed === CsvRow.Transformed(tagged, DataRow(marker, 43.0)))
  }
  
  test("Skipped") {
    val delegate = oneRow
    
    val marker = Variant.from("1_12345_A_T")
      
    val disp = Disposition.FlippedSameStrand
    
    val tagged = CsvRow.Tagged(delegate, marker, marker, disp)
    
    val dataRow = DataRow(marker, 42.0)
    
    def doTest(dataRowOpt: Option[DataRow]): Unit = {
      val skipped = CsvRow.Skipped(tagged, dataRowOpt)
      
      def incPvalue(dr: DataRow): DataRow = dr.copy(pvalue = dr.pvalue + 1.0)
      
      assert(skipped.isSkipped === true)
      assert(skipped.skip eq skipped)
      assert(skipped.transform(incPvalue) === skipped)
      assert(skipped.transform(incPvalue) eq skipped)
    }
     
    doTest(None)
    doTest(Some(dataRow))
  }
}
