package loamstream.loam.intake

import org.scalatest.FunSuite

/**
 * @author clint
 * Nov 13, 2020
 */
final class AggregatorRowExprTest extends FunSuite {
  test("Optional column defined but missing from input") {
    def doTest(failFast: Boolean): Unit = {
      val MRKR = ColumnName("MRKR")
      val PV = ColumnName("PV")
      val BT = ColumnName("BT")
      
      val markerDef = MarkerColumnDef(MRKR, MRKR.map(Variant.from))
      
      val metadata = AggregatorMetadata(
        dataset = "asdasdasd",
        phenotype = "akjdslfhsdf",
        ancestry = Ancestry.AA,
        tech = TechType.ExChip,
        quantitative = None)
      
      val expr = AggregatorRowExpr(
          metadata = metadata,
          failFast = failFast,
          markerDef = markerDef,
          pvalueDef = AggregatorColumnDefs.PassThru.pvalue(PV),
          betaDef = Some(AggregatorColumnDefs.PassThru.beta(BT)))
          
      val inputWithoutBetaColumn: Seq[DataRow] = Helpers.csvRows(
          Seq(MRKR.name, PV.name),
          Seq("1_1_A_T", "42"),
          Seq("1_2_A_T", "42"),
          Seq("1_3_A_T", "42"))
          
      val dataRows = Source.
                        fromIterable(inputWithoutBetaColumn).
                        tagFlips(markerDef, Helpers.FlipDetectors.NoFlipsEver).
                        map(expr)
  
      
      //Should fail, since we've declared a value for betaDef, but there is no beta column in the input
      if(failFast) {
        intercept[Exception] {
          dataRows.records.toList
        }
      } else {
        val results = dataRows.records.toList
        
        assert(results.map(_.isSkipped) === Seq(true, true, true))
      }
    }
    
    doTest(failFast = true)
    doTest(failFast = false)
  }
  
  test("Skipped input isn't transformed") {
    //Actual expr doesn't matter
    val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, LiteralColumnExpr("1_123_A_T").map(Variant.from))
    
    val metadata = AggregatorMetadata(
        dataset = "asdasdasd",
        phenotype = "akjdslfhsdf",
        ancestry = Ancestry.AA,
        tech = TechType.ExChip,
        quantitative = None)
    
    val expr = AggregatorRowExpr(
      metadata = metadata,
      failFast = true,
      markerDef = markerDef,
      pvalueDef = AggregatorColumnDefs.pvalue(LiteralColumnExpr(42.0))) //actual expr doesn't matter
      
    val input: Seq[DataRow] = Helpers.csvRows(
          Seq(AggregatorColumnNames.marker.name, AggregatorColumnNames.pvalue.name),
          Seq("1_1_A_T", "42"),
          Seq("1_2_A_T", "42"),
          Seq("1_3_A_T", "42"))
          
    val skippedDataRows = Source.
                            fromIterable(input).
                            tagFlips(markerDef, Helpers.FlipDetectors.NoFlipsEver).
                            map(_.skip).
                            map(expr).
                            records.
                            toList
                            
    assert(skippedDataRows.map(_.isSkipped) === Seq(true, true, true))
  }
}
