package loamstream.loam.intake

import org.scalatest.FunSuite
import scala.reflect.runtime.universe.TypeTag
import loamstream.loam.intake.flip.Disposition
import scala.collection.compat._

/**
 * @author clint
 * Nov 13, 2020
 */
final class AggregatorRowExprTest extends FunSuite {
  test("apply - happy path") {
    val metadata = AggregatorMetadata(
        bucketName = "some-bucket",
        topic = Option(UploadType.Variants),
        dataset = "asdasdasd",
        phenotype = "akjdslfhsdf",
        ancestry = Ancestry.AA,
        tech = TechType.ExChip,
        quantitative = None)
        
    def literal[A : TypeTag](a: A) = LiteralColumnExpr(a)
        
    def namedDef[A : TypeTag](name: String, a: A) = AnonColumnDef(literal(a))
    
    val v = Variant.from("12_345_A_T")
    
    val markerDef = {
      val MRKR = ColumnName("MRKR")
      
      MarkerColumnDef(MRKR, MRKR.flatMap(_ => literal(v)))
    }
    
    val expr = VariantRowExpr.PValueVariantRowExpr(
        metadata = metadata,
        markerDef = markerDef,
        pvalueDef = namedDef("pvalue", 1.2D),
        zscoreDef = Some(namedDef("zscore", 3.4D)),
        stderrDef = Some(namedDef("se", 5.6D)),
        betaDef = Some(namedDef("beta", 7.8D)),
        oddsRatioDef = Some(namedDef("or", 9.10D)),
        eafDef = Some(namedDef("eaf", 11.12D)),
        mafDef = Some(namedDef("maf", 13.14D)),
        nDef = Some(namedDef("n", 15.16D)))
    
    val csvRow = Helpers.csvRow()
        
    val row = VariantRow.Tagged(
        delegate = csvRow,
        marker = v,
        originalMarker = v,
        disposition = Disposition.NotFlippedSameStrand)
        
    val actual = expr(row)
    
    val expected = VariantRow.Transformed(
        derivedFrom = row,
        aggRow = PValueVariantRow(
          marker = v,
          pvalue = 1.2D,
          dataset = metadata.dataset,
          phenotype = metadata.phenotype,
          ancestry = metadata.ancestry,
          zscore = Some(3.4D),
          stderr = Some(5.6D),
          beta = Some(7.8D),
          oddsRatio = Some(9.10D),
          n = 15.16D,
          eaf = Some(11.12D),  
          maf = Some(13.14D),
          derivedFromRecordNumber = Some(csvRow.recordNumber)))
      
    assert(actual === expected)
  }
  
  test("Optional column defined but missing from input") {
    def doTest(failFast: Boolean): Unit = {
      val MRKR = ColumnName("MRKR")
      val PV = ColumnName("PV")
      val BT = ColumnName("BT")
      
      val markerDef = MarkerColumnDef(MRKR, MRKR.map(Variant.from))
      
      val metadata = AggregatorMetadata(
        bucketName = "some-bucket",
        topic = Option(UploadType.Variants),
        dataset = "asdasdasd",
        phenotype = "akjdslfhsdf",
        ancestry = Ancestry.AA,
        tech = TechType.ExChip,
        quantitative = None)
      
      val expr = VariantRowExpr.PValueVariantRowExpr(
          metadata = metadata,
          failFast = failFast,
          markerDef = markerDef,
          pvalueDef = AggregatorColumnDefs.PassThru.pvalue(PV),
          betaDef = Some(AggregatorColumnDefs.PassThru.beta(BT)),
          nDef = Some(AnonColumnDef(LiteralColumnExpr(99))))
          
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
          dataRows.records.to(List)
        }
      } else {
        val results = dataRows.records.to(List)
        
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
        bucketName = "some-bucket",
        topic = Option(UploadType.Variants),
        dataset = "asdasdasd",
        phenotype = "akjdslfhsdf",
        ancestry = Ancestry.AA,
        tech = TechType.ExChip,
        quantitative = None)
    
    val expr = VariantRowExpr.PValueVariantRowExpr(
      metadata = metadata,
      failFast = true,
      markerDef = markerDef,
      pvalueDef = AggregatorColumnDefs.pvalue(LiteralColumnExpr(42.0)), //actual expr doesn't matter
      nDef = Some(AnonColumnDef(LiteralColumnExpr(99)))) 
      
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
                            to(List)
                            
    assert(skippedDataRows.map(_.isSkipped) === Seq(true, true, true))
  }
}
