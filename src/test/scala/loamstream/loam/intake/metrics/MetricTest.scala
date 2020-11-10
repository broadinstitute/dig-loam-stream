package loamstream.loam.intake.metrics

import org.scalatest.FunSuite

import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.Dataset
import loamstream.loam.intake.Phenotype
import loamstream.loam.intake.flip.Disposition
import loamstream.loam.intake.flip.FlipDetector
import loamstream.util.Fold


/**
 * @author clint
 * May 5, 2020
 */
final class MetricTest extends FunSuite {
  import loamstream.loam.intake.IntakeSyntax._
  
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

  private val markerDef = NamedColumnDef(AggregatorColumnNames.marker, Marker, Marker)
  private val markerVariantDef = MarkerColumnDef(AggregatorColumnNames.marker, Marker.map(Variant.from))
  
  private val defaultRowExpr: AggregatorRowExpr = AggregatorRowExpr(
        markerDef = markerVariantDef,
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue))
  
  private val rowsNoFlips = {
    source.tagFlips(markerVariantDef, new MetricTest.MockFlipDetector(Set.empty)).map(defaultRowExpr)
  }
  
  test("countGreaterThan") {
    val gt2 = Metric.countGreaterThan(_._2.pvalue)(2)
    val gt4 = Metric.countGreaterThan(_._2.pvalue)(4)
    
    doMetricTest(gt4, expected = 2)(rowsNoFlips)
    doMetricTest(gt2, expected = 4)(rowsNoFlips)
  }
  
  test("fractionGreaterThan") {
    val fracGt2 = Metric.fractionGreaterThan(_._2.pvalue)(2)
    val fracGt4 = Metric.fractionGreaterThan(_._2.pvalue)(4)
    
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
    import AggregatorColumnNames._
    
    val csvData = s"""|${marker.name} ${beta.name} ${stderr.name} ${zscore.name} ${Pvalue.name}
                      |${Vars.x} 4 2 2 99
                      |${Vars.y} 8 2 4 99
                      |${Vars.z} 8 4 42 99
                      |${Vars.a} 5 2.5 42 99
                      |${Vars.b} 8 4 2 99
                      |${Vars.c} 9 2 4.5 99""".stripMargin
  
    val source = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)
    
    val flipDetector = new MetricTest.MockFlipDetector(Set.empty)
                      
    val toAggregatorFormat: AggregatorRowExpr = AggregatorRowExpr(
        markerDef = MarkerColumnDef(marker, marker.map(Variant.from)),
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        zscoreDef = Some(AggregatorColumnDefs.zscore(zscore)),
        stderrDef = Some(AggregatorColumnDefs.stderr(stderr)),
        betaDef = Some(AggregatorColumnDefs.beta(beta)))
    
    val rows = source.tagFlips(toAggregatorFormat.markerDef, flipDetector).map(toAggregatorFormat)
        
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore()
                      
    doMetricTest(countDisagreements, expected = 2)(rows)
  }
  
  test("countWithDisagreeingBetaStderrZscore - some flips") {
    import AggregatorColumnNames._
    
    val csvData = s"""|${Marker.name} ${beta.name} ${stderr.name} ${zscore.name} ${Pvalue.name}
                      |${Vars.x} 4 2 2 99   
                      |${Vars.y} 8 2 -4 99
                      |${Vars.z} 8 4 42 99
                      |${Vars.a} 5 2.5 42 99
                      |${Vars.b} 8 4 -2 99
                      |${Vars.c} 9 2 4.5 99""".stripMargin
  
    val source = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)
    
    val flipDetector = new MetricTest.MockFlipDetector(Set(Vars.y, Vars.a, Vars.b).map(Variant.from))
    
    val toAggregatorFormat: AggregatorRowExpr = AggregatorRowExpr(
        markerDef = markerVariantDef,
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        zscoreDef = Some(AggregatorColumnDefs.zscore(zscore)),
        stderrDef = Some(AggregatorColumnDefs.stderr(stderr)),
        betaDef = Some(AggregatorColumnDefs.beta(beta)))
                      
    val rows = source.tagFlips(markerVariantDef, flipDetector).map(toAggregatorFormat)
                      
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore()
                      
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
    
    val toAggregatorFormat: AggregatorRowExpr = AggregatorRowExpr(
        markerDef = MarkerColumnDef(Marker, marker.map(Variant.from)),
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        zscoreDef = Some(AggregatorColumnDefs.zscore(zscore)),
        stderrDef = Some(AggregatorColumnDefs.stderr(stderr)),
        betaDef = Some(AggregatorColumnDefs.beta(beta)))
                      
    val rows = source.tagFlips(toAggregatorFormat.markerDef, flipDetector).map(toAggregatorFormat)
                      
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore()
                      
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
    
    val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, marker.map(Variant.from))
    
    val toAggregatorFormat: AggregatorRowExpr = AggregatorRowExpr(
        markerDef = markerDef,
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        zscoreDef = Some(AggregatorColumnDefs.zscore(zscore)),
        stderrDef = Some(AggregatorColumnDefs.stderr(stderr)),
        betaDef = Some(AggregatorColumnDefs.beta(beta)))
    
    val flipDetector = new MetricTest.MockFlipDetector(Set(Vars.y, Vars.a, Vars.b).map(Variant.from))
        
    val rows = source.tagFlips(markerDef, flipDetector).map(toAggregatorFormat)
    
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore()
                      
    doMetricTest(countDisagreements, expected = 2)(rows)
  }
  
  test("countWithDisagreeingBetaStderrZscore - missing fields counds as agreement") {
    val marker = ColumnName("lalala")
    
    val csvData = s"""|${marker.name} ${Pvalue.name}
                      |${Vars.x} 99
                      |${Vars.y} 99
                      |${Vars.z} 99
                      |${Vars.a} 99
                      |${Vars.b} 99
                      |${Vars.c} 99""".stripMargin
  
    val source = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)
    
    val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, marker.map(Variant.from))
    
    val toAggregatorFormat: AggregatorRowExpr = AggregatorRowExpr(
        markerDef = markerDef,
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue))
    
    val flipDetector = new MetricTest.MockFlipDetector(Set(Vars.y, Vars.a, Vars.b).map(Variant.from))
        
    val rows = source.tagFlips(markerDef, flipDetector).map(toAggregatorFormat)
    
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore()
                      
    doMetricTest(countDisagreements, expected = 0)(rows)
  }
  
  test("mean") {
    val mean = Metric.mean(_._2.pvalue)
    
    doMetricTest(mean, expected = (1 + 2 + 3 + 4 + 5 + 6) / 6.0)(rowsNoFlips)
  }
  
  private def doMetricTest[A](metric: Metric[A], expected: A)(rows: Source[(CsvRow.WithFlipTag, DataRow)]): Unit = {
    assert(Fold.fold(rows.records)(metric) === expected)
    
    assert(Fold.fold(rows.records.toList)(metric) === expected)
  }
}

object MetricTest {
  import loamstream.loam.intake.Variant
  
  private final class MockBioIndexClient(knownVariants: Set[String]) extends BioIndexClient {
    override def isKnown(variant: Variant): Boolean = knownVariants.contains(variant.underscoreDelimited)
    
    override def isKnown(dataset: Dataset): Boolean = ???
  
    override def isKnown(phenotype: Phenotype): Boolean = ???
    
    override def findClosestMatch(dataset: Dataset): Option[Dataset] = ???
  
    override def findClosestMatch(phenotype: Phenotype): Option[Phenotype] = ???
  }
  
  private final class MockFlipDetector(flippedVariants: Set[Variant]) extends FlipDetector {
    override def isFlipped(variantId: Variant): Disposition = {
      if(flippedVariants.contains(variantId)) Disposition.FlippedSameStrand else Disposition.NotFlippedSameStrand
    }
  }
}
