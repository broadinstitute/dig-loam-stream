package loamstream.loam.intake.metrics

import org.scalatest.FunSuite
import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.flip.FlipDetector
import loamstream.util.Fold
import loamstream.loam.intake.aggregator.ColumnNames
import loamstream.loam.intake.ColumnDef
import loamstream.loam.intake.aggregator.RowExpr
import loamstream.loam.intake.aggregator.ColumnDefs
import loamstream.loam.intake.flip.Disposition


/**
 * @author clint
 * May 5, 2020
 */
final class MetricTest extends FunSuite {
  import IntakeSyntax._
  
  private val Marker = ColumnName("mrkr")
  private val Pvalue = ColumnName("p_value")
  
  object Vars {
    val x = "1_12345_A_T"
    val y = "1_1234_A_T"
    val z = "1_123_A_T"
    val a = "1_12_A_T"
    val b = "1_1_A_T"
    val c = "1_123451_A_T"
  }
  
  private val csvData = s"""|${Marker.name} ${Pvalue.name}
                            |${Vars.x} 6
                            |${Vars.y} 4
                            |${Vars.z} 3
                            |${Vars.a} 5
                            |${Vars.b} 2
                            |${Vars.c} 1""".stripMargin
  
  private val source = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)

  private val markerDef = NamedColumnDef(ColumnNames.marker, Marker, Marker)
  
  private val defaultRowExpr: RowExpr = RowExpr(
        markerDef = markerDef,
        pvalueDef = ColumnDefs.pvalue(Pvalue))
  
  private val rowsNoFlips = source.tagFlips(markerDef, new MetricTest.MockFlipDetector(Set.empty)).map(defaultRowExpr)
  
  test("countGreaterThan") {
    val gt2 = Metric.countGreaterThan(_.pvalue)(2)
    val gt4 = Metric.countGreaterThan(_.pvalue)(4)
    
    doMetricTest(gt4, expected = 2)(rowsNoFlips)
    doMetricTest(gt2, expected = 4)(rowsNoFlips)
  }
  
  test("fractionGreaterThan") {
    val fracGt2 = Metric.fractionGreaterThan(_.pvalue)(2)
    val fracGt4 = Metric.fractionGreaterThan(_.pvalue)(4)
    
    doMetricTest(fracGt4, expected = (2d / 6d))(rowsNoFlips)
    doMetricTest(fracGt2, expected = (4d / 6d))(rowsNoFlips)
  }
  
  test("fractionUnknown") {
    val bioIndexClient: BioIndexClient = new MetricTest.MockBioIndexClient(knownVariants = Set(Vars.x, Vars.z, Vars.a))
                      
    val fracUnknown = Metric.fractionUnknown(client = bioIndexClient)
   
    doMetricTest(fracUnknown, expected = (3.0 / 6.0))(rowsNoFlips)
  }
  
  test("countKnown") {
    val bioIndexClient: BioIndexClient = new MetricTest.MockBioIndexClient(knownVariants = Set(Vars.z, Vars.a))
                      
    val countKnown = Metric.countKnown(client = bioIndexClient)
   
    doMetricTest(countKnown, expected = 2)(rowsNoFlips)
  }
  
  test("countUnknown") {
    val bioIndexClient: BioIndexClient = new MetricTest.MockBioIndexClient(knownVariants = Set(Vars.z, Vars.a))
                      
    val countUnknown = Metric.countUnknown(client = bioIndexClient)
   
    doMetricTest(countUnknown, expected = 4)(rowsNoFlips)
  }
  
  test("countWithDisagreeingBetaStderrZscore - no flips") {
    import _root_.loamstream.loam.intake.aggregator.ColumnNames._
    
    val csvData = s"""|${marker.name} ${beta.name} ${stderr.name} ${zscore.name} ${Pvalue.name}
                      |${Vars.x} 4 2 2 99
                      |${Vars.y} 8 2 4 99
                      |${Vars.z} 8 4 42 99
                      |${Vars.a} 5 2.5 42 99
                      |${Vars.b} 8 4 2 99
                      |${Vars.c} 9 2 4.5 99""".stripMargin
  
    val source = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)
    
    val flipDetector = new MetricTest.MockFlipDetector(Set.empty)
                      
    val toAggregatorFormat: RowExpr = RowExpr(
        markerDef = NamedColumnDef(marker, marker),
        pvalueDef = ColumnDefs.pvalue(Pvalue),
        zscoreDef = Some(ColumnDefs.zscore(zscore)),
        stderrDef = Some(ColumnDefs.stderr(stderr)),
        betaDef = Some(ColumnDefs.beta(beta)))
    
    val rows = source.tagFlips(toAggregatorFormat.markerDef, flipDetector).map(toAggregatorFormat)
        
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore(flipDetector)
                      
    doMetricTest(countDisagreements, expected = 2)(rows)
  }
  
  test("countWithDisagreeingBetaStderrZscore - some flips") {
    import _root_.loamstream.loam.intake.aggregator.ColumnNames._
    
    val csvData = s"""|${Marker.name} ${beta.name} ${stderr.name} ${zscore.name} ${Pvalue.name}
                      |${Vars.x} 4 2 2 99
                      |${Vars.y} 8 2 -4 99
                      |${Vars.z} 8 4 42 99
                      |${Vars.a} 5 2.5 42 99
                      |${Vars.b} 8 4 -2 99
                      |${Vars.c} 9 2 4.5 99""".stripMargin
  
    val source = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)
    
    val flipDetector = new MetricTest.MockFlipDetector(Set(Vars.y, Vars.a, Vars.b))
    
    val toAggregatorFormat: RowExpr = RowExpr(
        markerDef = markerDef,
        pvalueDef = ColumnDefs.pvalue(Pvalue),
        zscoreDef = Some(ColumnDefs.zscore(zscore)),
        stderrDef = Some(ColumnDefs.stderr(stderr)),
        betaDef = Some(ColumnDefs.beta(beta)))
                      
    val rows = source.tagFlips(markerDef, flipDetector).map(toAggregatorFormat)
                      
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore(flipDetector)
                      
    doMetricTest(countDisagreements, expected = 2)(rows)
  }
  
  test("countWithDisagreeingBetaStderrZscore - no flips, non-default names") {
    val marker = ColumnName("lalala")
    val beta = ColumnName("blerg")
    val stderr = ColumnName("BAZ")
    val zscore = ColumnName("Bipp")
    
    val csvData = s"""|${marker.name} ${beta.name} ${stderr.name} ${zscore.name} ${Pvalue.name}
                      |${Vars.x} 4 2 2 99
                      |${Vars.y} 8 2 4 99
                      |${Vars.z} 8 4 42 99
                      |${Vars.a} 5 2.5 42 99
                      |${Vars.b} 8 4 2 99
                      |${Vars.c} 9 2 4.5 99""".stripMargin
  
    val source = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)
    
    val flipDetector = new MetricTest.MockFlipDetector(Set.empty)
    
    val toAggregatorFormat: RowExpr = RowExpr(
        markerDef = NamedColumnDef(Marker, marker, marker),
        pvalueDef = ColumnDefs.pvalue(Pvalue),
        zscoreDef = Some(ColumnDefs.zscore(zscore)),
        stderrDef = Some(ColumnDefs.stderr(stderr)),
        betaDef = Some(ColumnDefs.beta(beta)))
                      
    val rows = source.tagFlips(toAggregatorFormat.markerDef, flipDetector).map(toAggregatorFormat)
                      
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore(flipDetector)
                      
    doMetricTest(countDisagreements, expected = 2)(rows)
  }
  
  test("countWithDisagreeingBetaStderrZscore - some flips, non-default names") {
    val marker = ColumnName("lalala")
    val beta = ColumnName("BAT")
    val stderr = ColumnName("BAZ")
    val zscore = ColumnName("Bipp")
    
    val csvData = s"""|${marker.name} ${beta.name} ${stderr.name} ${zscore.name} ${Pvalue.name}
                      |${Vars.x} 4 2 2 99
                      |${Vars.y} 8 2 -4 99
                      |${Vars.z} 8 4 42 99
                      |${Vars.a} 5 2.5 42 99
                      |${Vars.b} 8 4 -2 99
                      |${Vars.c} 9 2 4.5 99""".stripMargin
  
    val source = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)
    
    val markerDef = NamedColumnDef(ColumnNames.marker, marker, marker)
    
    val toAggregatorFormat: RowExpr = RowExpr(
        markerDef = markerDef,
        pvalueDef = ColumnDefs.pvalue(Pvalue),
        zscoreDef = Some(ColumnDefs.zscore(zscore)),
        stderrDef = Some(ColumnDefs.stderr(stderr)),
        betaDef = Some(ColumnDefs.beta(beta)))
    
    val flipDetector = new MetricTest.MockFlipDetector(Set(Vars.y, Vars.a, Vars.b))
        
    val rows = source.tagFlips(markerDef, flipDetector).map(toAggregatorFormat)
    
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore(flipDetector)
                      
    doMetricTest(countDisagreements, expected = 2)(rows)
  }
  
  test("mean") {
    val mean = Metric.mean(_.pvalue)
    
    doMetricTest(mean, expected = (1 + 2 + 3 + 4 + 5 + 6) / 6.0)(rowsNoFlips)
  }
  
  private def doMetricTest[A](metric: Metric[A], expected: A)(rows: Source[DataRow]): Unit = {
    assert(Fold.fold(rows.records)(metric) === expected)
    
    assert(Fold.fold(rows.records.toList)(metric) === expected)
  }
}

object MetricTest {
  private final class MockBioIndexClient(knownVariants: Set[String]) extends BioIndexClient {
    override def isKnown(varId: String): Boolean = knownVariants.contains(varId)
  }
  
  private final class MockFlipDetector(flippedVariants: Set[String]) extends FlipDetector {
    override def isFlipped(variantId: String): Disposition = {
      if(flippedVariants.contains(variantId)) Disposition.FlippedSameStrand else Disposition.NotFlippedSameStrand
    }
  }
}
