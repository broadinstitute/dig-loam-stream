package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.loam.intake.flip.Disposition

/**
 * @author clint
 * Dec 1, 2020
 */
final class VariantRowTest extends FunSuite with CsvRowTest.RowHelpers {
  test("Tagged") {
    val delegate = oneRow
    
    def doTest(disp: Disposition): Unit = {
      val marker = Variant.from("1_12345_A_T")
      
      val tagged = VariantRow.Tagged(delegate, marker, marker, disp)
      
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
    
    val tagged = VariantRow.Tagged(delegate, marker, marker, disp)
    
    val dataRow = AggregatorVariantRow(marker, 42.0, dataset = "asd", phenotype = "fdg", ancestry = Ancestry.AA)
    
    val transformed = VariantRow.Transformed(tagged, dataRow)
    
    assert(transformed.isSkipped === false)
    
    assert(transformed.skip === VariantRow.Skipped(tagged, Some(dataRow)))
    
    def incPvalue(dr: AggregatorVariantRow): AggregatorVariantRow = dr.copy(pvalue = dr.pvalue + 1.0) 
    
    val furtherTransformed = transformed.transform(incPvalue)
    
    {
      val expected = VariantRow.Transformed(
          tagged, 
          AggregatorVariantRow(marker, 43.0, dataset = "asd", phenotype = "fdg", ancestry = Ancestry.AA))

      assert(furtherTransformed === expected)
    }
  }
  
  test("Skipped") {
    val delegate = oneRow
    
    val marker = Variant.from("1_12345_A_T")
      
    val disp = Disposition.FlippedSameStrand
    
    val tagged = VariantRow.Tagged(delegate, marker, marker, disp)
    
    val dataRow = AggregatorVariantRow(marker, 42.0, dataset = "asd", phenotype = "fdg", ancestry = Ancestry.AA)
    
    def doTest(dataRowOpt: Option[AggregatorVariantRow]): Unit = {
      val skipped = VariantRow.Skipped(tagged, dataRowOpt)
      
      def incPvalue(dr: AggregatorVariantRow): AggregatorVariantRow = dr.copy(pvalue = dr.pvalue + 1.0)
      
      assert(skipped.isSkipped === true)
      assert(skipped.skip eq skipped)
      assert(skipped.transform(incPvalue) === skipped)
      assert(skipped.transform(incPvalue) eq skipped)
    }
     
    doTest(None)
    doTest(Some(dataRow))
  }
}
