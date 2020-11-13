package loamstream.loam.intake

import org.scalatest.FunSuite

/**
 * @author clint
 * Nov 13, 2020
 */
final class AggregatorRowExprTest extends FunSuite {
  test("Optional column defined but missing from input") {
    val MRKR = ColumnName("MRKR")
    val PV = ColumnName("PV")
    val BT = ColumnName("BT")
    
    val markerDef = MarkerColumnDef(MRKR, MRKR.map(Variant.from))
    
    val expr = AggregatorRowExpr(
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
    intercept[Exception] {
      dataRows.records.toList
    }
  }
}
