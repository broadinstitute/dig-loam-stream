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
      
      val expr = AggregatorRowExpr(
          failFast = failFast,
          markerDef = markerDef,
          pvalueDef = AggregatorColumnDefs.PassThru.pvalue(PV),
          betaDef = Some(AggregatorColumnDefs.PassThru.beta(BT)))
          
      val inputWithoutBetaColumn: Seq[CsvRow] = Helpers.csvRows(
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
}
