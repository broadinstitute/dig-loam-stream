package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.loam.intake.flip.Disposition
import scala.collection.compat._

/**
 * @author clint
 * Dec 1, 2020
 */
final class VariantRowTest extends FunSuite with CsvRowTest.RowHelpers {
  test("Tagged") {
    val delegate = oneRow
    
    def doTest(disp: Disposition): Unit = {
      val marker = Variant.from("1_12345_A_T")
      
      val tagged = VariantRow.Analyzed.Tagged(delegate, marker, marker, disp)
      
      assert(tagged.isSkipped === false)
      assert(tagged.notSkipped === true)
      
      assert(tagged.derivedFrom eq delegate)
      
      assert(tagged.isFlipped === disp.isFlipped)
      assert(tagged.notFlipped === disp.notFlipped)
      assert(tagged.isSameStrand === disp.isSameStrand)
      assert(tagged.isComplementStrand === disp.isComplementStrand)
      
      assert(tagged.derivedFrom.size === delegate.size)
      
      assert(tagged.derivedFrom.values.to(List) === delegate.values.to(List))
      
      assert(tagged.derivedFrom.recordNumber === delegate.recordNumber)
      
      assert(tagged.derivedFrom.getFieldByIndex(0) === "1")
      assert(tagged.derivedFrom.getFieldByIndex(1) === "hello")
      assert(tagged.derivedFrom.getFieldByIndex(2) === "blah_123")
      
      val skipped = tagged.skip
      
      assert(skipped ne tagged)
      
      assert(skipped.isSkipped === true)
      assert(skipped.notSkipped === false)
      
      assert(skipped.derivedFrom.values.to(List) === tagged.derivedFrom.values.to(List))
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
    
    val tagged = VariantRow.Analyzed.Tagged(delegate, marker, marker, disp)
    
    val dataRow = PValueVariantRow(marker, 42.0, dataset = "asd", phenotype = "fdg", ancestry = Ancestry.AA, n = 42)
    
    val transformed = VariantRow.Parsed.Transformed(tagged.derivedFrom, tagged, dataRow)
    
    assert(transformed.isSkipped === false)
    
    assert(transformed.skip === VariantRow.Parsed.Skipped(tagged.derivedFrom, Some(tagged), Some(dataRow)))
    
    def incPvalue(dr: PValueVariantRow): PValueVariantRow = dr.copy(pvalue = dr.pvalue + 1.0) 
    
    val furtherTransformed = transformed.transform(incPvalue)
    
    {
      val expected = VariantRow.Parsed.Transformed(
          tagged.derivedFrom,
          tagged, 
          PValueVariantRow(marker, 43.0, dataset = "asd", phenotype = "fdg", ancestry = Ancestry.AA, n = 42))

      assert(furtherTransformed === expected)
    }
  }
  
  test("Skipped") {
    val delegate = oneRow
    
    val marker = Variant.from("1_12345_A_T")
      
    val disp = Disposition.FlippedSameStrand
    
    val tagged = VariantRow.Analyzed.Tagged(delegate, marker, marker, disp)
    
    val dataRow = PValueVariantRow(marker, 42.0, dataset = "asd", phenotype = "fdg", ancestry = Ancestry.AA, n = 42)
    
    def doTest(dataRowOpt: Option[PValueVariantRow]): Unit = {
      val skipped = VariantRow.Parsed.Skipped(tagged.derivedFrom, Some(tagged), dataRowOpt)
      
      def incPvalue(dr: PValueVariantRow): PValueVariantRow = dr.copy(pvalue = dr.pvalue + 1.0)
      
      assert(skipped.isSkipped === true)
      assert(skipped.skip eq skipped)
      assert(skipped.transform(incPvalue) === skipped)
      assert(skipped.transform(incPvalue) eq skipped)
    }
     
    doTest(None)
    doTest(Some(dataRow))
  }
}
