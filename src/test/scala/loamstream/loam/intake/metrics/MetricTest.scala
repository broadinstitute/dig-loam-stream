package loamstream.loam.intake.metrics

import org.scalatest.FunSuite

import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.Dataset
import loamstream.loam.intake.Phenotype
import loamstream.loam.intake.flip.Disposition
import loamstream.loam.intake.flip.FlipDetector
import loamstream.util.Fold
import loamstream.loam.intake.Helpers
import loamstream.loam.intake.Chromosomes
import loamstream.TestHelpers
import loamstream.util.Files
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import loamstream.loam.intake.RowSink
import loamstream.loam.intake.RenderableRow
import loamstream.loam.intake.BaseVariantRow
import loamstream.loam.intake.LiteralColumnExpr
import scala.collection.compat._


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
  
  private val metadata = AggregatorMetadata(
    bucketName = "some-bucket",
    topic = Option(UploadType.Variants),
    dataset = "asdasdasd",
    phenotype = "akjdslfhsdf",
    ancestry = Ancestry.AA,
    tech = TechType.ExChip,
    quantitative = None)
  
  private val csvData = s"""|${Marker.name} ${Pvalue.name}
                            |${Vars.x} 6
                            |${Vars.y} 4
                            |${Vars.z} 3
                            |${Vars.a} 5
                            |${Vars.b} 2
                            |${Vars.c} 1""".stripMargin
  
  private val source = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)

  private val markerDef = AnonColumnDef(Marker, Marker)
  private val markerVariantDef = MarkerColumnDef(AggregatorColumnNames.marker, Marker.map(Variant.from))
  
  private val defaultRowExpr: PValueVariantRowExpr = PValueVariantRowExpr(
      metadata = metadata,
      markerDef = markerVariantDef,
      pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
      nDef = Some(AnonColumnDef(LiteralColumnExpr(99))))

  private val rowsNoFlips = {
    source.tagFlips(markerVariantDef, new MetricTest.MockFlipDetector(Set.empty)).map(defaultRowExpr)
  }
  
  private val zeroedChromosomeCounts: Map[String, Int] = Chromosomes.names.map(_ -> 0).toMap                            
                            
  test("countGreaterThan") {
    val gt2 = Metric.countGreaterThan[PValueVariantRow](_.aggRow.pvalue)(2)
    val gt4 = Metric.countGreaterThan[PValueVariantRow](_.aggRow.pvalue)(4)
    
    doMetricTest(gt4, expected = 2)(rowsNoFlips)
    doMetricTest(gt2, expected = 4)(rowsNoFlips)
  }
  
  test("fractionGreaterThan") {
    val fracGt2 = Metric.fractionGreaterThan[PValueVariantRow](_.aggRow.pvalue)(2)
    val fracGt4 = Metric.fractionGreaterThan[PValueVariantRow](_.aggRow.pvalue)(4)
    
    doMetricTest(fracGt4, expected = (2d / 6d))(rowsNoFlips)
    doMetricTest(fracGt2, expected = (4d / 6d))(rowsNoFlips)
  }
  
  test("fractionUnknown") {
    val bioIndexClient: BioIndexClient = new MetricTest.MockBioIndexClient(knownVariants = Set(Vars.x, Vars.z, Vars.a))
                      
    val fracUnknown = Metric.fractionUnknown[PValueVariantRow](client = bioIndexClient)
   
    doMetricTest(fracUnknown, expected = (3.0 / 6.0))(rowsNoFlips)
  }
  
  test("countKnown") {
    val bioIndexClient: BioIndexClient = new MetricTest.MockBioIndexClient(knownVariants = Set(Vars.z, Vars.a))
                      
    val countKnown = Metric.countKnown[PValueVariantRow](client = bioIndexClient)
   
    doMetricTest(countKnown, expected = 2)(rowsNoFlips)
  }
  
  test("countUnknown") {
    val bioIndexClient: BioIndexClient = new MetricTest.MockBioIndexClient(knownVariants = Set(Vars.z, Vars.a))
                      
    val countUnknown = Metric.countUnknown[PValueVariantRow](client = bioIndexClient)
   
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
                      
    val toAggregatorFormat: PValueVariantRowExpr = VariantRowExpr.PValueVariantRowExpr(
        metadata = metadata,
        markerDef = MarkerColumnDef(marker, marker.map(Variant.from)),
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        zscoreDef = Some(AggregatorColumnDefs.zscore(zscore)),
        stderrDef = Some(AggregatorColumnDefs.stderr(stderr)),
        betaDef = Some(AggregatorColumnDefs.beta(beta)),
        nDef = Some(AnonColumnDef(LiteralColumnExpr(99))))
    
    val rows = source.tagFlips(toAggregatorFormat.markerDef, flipDetector).map(toAggregatorFormat)
        
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore[PValueVariantRow]()
                      
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
    
    val toAggregatorFormat: PValueVariantRowExpr = VariantRowExpr.PValueVariantRowExpr(
        metadata = metadata,
        markerDef = markerVariantDef,
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        zscoreDef = Some(AggregatorColumnDefs.zscore(zscore)),
        stderrDef = Some(AggregatorColumnDefs.stderr(stderr)),
        betaDef = Some(AggregatorColumnDefs.beta(beta)),
        nDef = Some(AnonColumnDef(LiteralColumnExpr(99))))
                      
    val rows = source.tagFlips(markerVariantDef, flipDetector).map(toAggregatorFormat)
                      
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore[PValueVariantRow]()
                      
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
    
    val toAggregatorFormat: PValueVariantRowExpr = VariantRowExpr.PValueVariantRowExpr(
        metadata = metadata,
        markerDef = MarkerColumnDef(Marker, marker.map(Variant.from)),
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        zscoreDef = Some(AggregatorColumnDefs.zscore(zscore)),
        stderrDef = Some(AggregatorColumnDefs.stderr(stderr)),
        betaDef = Some(AggregatorColumnDefs.beta(beta)),
        nDef = Some(AnonColumnDef(LiteralColumnExpr(99))))
                      
    val rows = source.tagFlips(toAggregatorFormat.markerDef, flipDetector).map(toAggregatorFormat)
                      
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore[PValueVariantRow]()
                      
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
    
    val toAggregatorFormat: PValueVariantRowExpr = VariantRowExpr.PValueVariantRowExpr(
        metadata = metadata,
        markerDef = markerDef,
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        zscoreDef = Some(AggregatorColumnDefs.zscore(zscore)),
        stderrDef = Some(AggregatorColumnDefs.stderr(stderr)),
        betaDef = Some(AggregatorColumnDefs.beta(beta)),
        nDef = Some(AnonColumnDef(LiteralColumnExpr(99))))
    
    val flipDetector = new MetricTest.MockFlipDetector(Set(Vars.y, Vars.a, Vars.b).map(Variant.from))
        
    val rows = source.tagFlips(markerDef, flipDetector).map(toAggregatorFormat)
    
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore[PValueVariantRow]()
                      
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
    
    val toAggregatorFormat: PValueVariantRowExpr = VariantRowExpr.PValueVariantRowExpr(
        metadata = metadata,
        markerDef = markerDef,
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        nDef = Some(AnonColumnDef(LiteralColumnExpr(99))))
    
    val flipDetector = new MetricTest.MockFlipDetector(Set(Vars.y, Vars.a, Vars.b).map(Variant.from))
        
    val rows = source.tagFlips(markerDef, flipDetector).map(toAggregatorFormat)
    
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore[PValueVariantRow]()
                      
    doMetricTest(countDisagreements, expected = 0)(rows)
  }
  
  test("mean") {
    val mean = Metric.mean[PValueVariantRow, Double](_.aggRow.pvalue)
    
    doMetricTest(mean, expected = (1 + 2 + 3 + 4 + 5 + 6) / 6.0)(rowsNoFlips)
  }
  
  test("countByChromosome") {
    def doTest(anySkips: Boolean, countSkipped: Boolean): Unit = {
      val marker = ColumnName("lalala")
    
      val csvData = s"""|${marker.name} ${Pvalue.name}
                        |1_123_A_T 99
                        |2_123_A_T 99
                        |1_124_A_T 99
                        |11_123_A_T 99
                        |11_124_A_T 99
                        |1_125_A_T 99
                        |MT_123_A_T 99""".stripMargin
    
      val source = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)
      
      val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, marker.map(Variant.from))
      
      val toAggregatorFormat: PValueVariantRowExpr = VariantRowExpr.PValueVariantRowExpr(
          metadata = metadata,
          markerDef = markerDef,
          pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
          nDef = Some(AnonColumnDef(LiteralColumnExpr(99))))
      
      val skipped: Set[Variant] = if(anySkips) Set(Variant("2_123_A_T"), Variant("1_124_A_T")) else Set.empty
          
      val flipDetector = Helpers.FlipDetectors.NoFlipsEver
          
      val rows = source.tagFlips(markerDef, flipDetector).map(toAggregatorFormat).map { row =>
        if(skipped.contains(row.derivedFrom.marker)) row.skip else row
      }
      
      val m = Metric.countByChromosome[PValueVariantRow](countSkipped = countSkipped)
      
      val expected: Map[String, Int] = zeroedChromosomeCounts ++ {
        val countEverything = countSkipped || !anySkips
        
        if(countEverything) { Seq("1" -> 3, "2" -> 1, "11" -> 2, "MT" -> 1) }
        else { Seq("1" -> 2, "11" -> 2, "MT" -> 1) }
      }
      
      doMetricTest(m, expected)(rows)
    }
    
    doTest(anySkips = true, countSkipped = true)
    doTest(anySkips = true, countSkipped = false)
    doTest(anySkips = false, countSkipped = true)
    doTest(anySkips = false, countSkipped = false)
  }
  
  private def doFlippedComplementedTest(
      metric: Metric[PValueVariantRow, Int],
      expected: Int,
      flipped: Set[Variant], 
      complemented: Set[Variant]): Unit = {
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
    
    val toAggregatorFormat: PValueVariantRowExpr = VariantRowExpr.PValueVariantRowExpr(
        metadata = metadata,
        markerDef = markerDef,
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        nDef = Some(AnonColumnDef(LiteralColumnExpr(99))))
    
    val flipDetector = MetricTest.MockFlipDetector(flippedVariants = flipped, complementedVariants = complemented)
        
    val rows = source.tagFlips(markerDef, flipDetector).map(toAggregatorFormat)
    
    doMetricTest(metric, expected)(rows)
  }
  
  test("countFlipped") {
    val flipped = Set(Vars.x, Vars.z, Vars.a).map(Variant.from)
    
    doFlippedComplementedTest(Metric.countFlipped, flipped.size, flipped = flipped, complemented = Set.empty)
  }
  
  test("countComplemented") {
    val complemented = Set(Vars.x, Vars.z, Vars.a).map(Variant.from)
    
    doFlippedComplementedTest(
        Metric.countComplemented, complemented.size, flipped = Set.empty, complemented = complemented)
  }
  
  private def doSkippedTest(
      metric: Metric[PValueVariantRow, Int],
      expected: Int,
      skipped: Set[Variant]): Unit = {
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
    
    val toAggregatorFormat: PValueVariantRowExpr = VariantRowExpr.PValueVariantRowExpr(
        metadata = metadata,
        markerDef = markerDef,
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        nDef = Some(AnonColumnDef(LiteralColumnExpr(99))))
    
    val flipDetector = Helpers.FlipDetectors.NoFlipsEver
    
    val parsedRows = source.tagFlips(markerDef, flipDetector).map(toAggregatorFormat)
    
    val withSomeSkips = parsedRows.map { row =>
      if(skipped.contains(row.aggRowOpt.get.asInstanceOf[PValueVariantRow].marker)) row.skip else row
    }
    
    doMetricTest(metric, expected = expected)(withSomeSkips)
  }
  
  test("countSkipped / countNOTSkipped") {
    doSkippedTest(Metric.countSkipped, 3, Set(Vars.x, Vars.z, Vars.a).map(Variant.from))
    
    doSkippedTest(Metric.countNOTSkipped, 4, Set(Vars.x, Vars.a).map(Variant.from))
  }
  
  test("summaryStats") {
    val marker = ColumnName("lalala")
    
    val v0 = Variant("1_1_A_T")
    val v1 = Variant("1_2_A_T")
    val v2 = Variant("2_1_A_T")
    val v3 = Variant("2_2_A_T")
    val v4 = Variant("3_1_A_T")
    val v5 = Variant("4_1_A_T")
    
    assert(Set(v0, v1, v2, v3, v4, v5).size === 6)
    
    val csvData = s"""|${marker.name} ${Pvalue.name}
                      |${v0.underscoreDelimited} 99
                      |${v1.underscoreDelimited} 99
                      |${v2.underscoreDelimited} 99
                      |${v3.underscoreDelimited} 99
                      |${v4.underscoreDelimited} 99
                      |${v5.underscoreDelimited} 99""".stripMargin
  
    val source = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)
    
    val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, marker.map(Variant.from))
    
    val toAggregatorFormat: PValueVariantRowExpr = VariantRowExpr.PValueVariantRowExpr(
        metadata = metadata,
        markerDef = markerDef,
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        nDef = Some(AnonColumnDef(LiteralColumnExpr(99))))
    
    val flipDetector = MetricTest.MockFlipDetector(
        flippedVariants = Set(v0, v3), complementedVariants = Set(v1, v3))
    
    val skipped = Set(v1, v4)
        
    val rows = source.tagFlips(markerDef, flipDetector).map(toAggregatorFormat).map { row =>
      if(skipped.contains(row.derivedFrom.originalMarker)) row.skip else row
    }
        
    val expected = SummaryStats(
      validVariants = 6 - skipped.size,
      skippedVariants = skipped.size,
      flippedVariants = 2,
      complementedVariants = 2,
      validVariantsByChromosome = zeroedChromosomeCounts ++ Seq("1" -> 1, "2" -> 2, "4" -> 1))
      
    doMetricTest(Metric.summaryStats[PValueVariantRow], expected)(rows)
  }
  
  test("writeSummaryStatsTo") {
    val marker = ColumnName("lalala")
    
    val v0 = Variant("1_1_A_T")
    val v1 = Variant("1_2_A_T")
    val v2 = Variant("2_1_A_T")
    val v3 = Variant("2_2_A_T")
    val v4 = Variant("3_1_A_T")
    val v5 = Variant("4_1_A_T")
    
    assert(Set(v0, v1, v2, v3, v4, v5).size === 6)
    
    val csvData = s"""|${marker.name} ${Pvalue.name}
                      |${v0.underscoreDelimited} 99
                      |${v1.underscoreDelimited} 99
                      |${v2.underscoreDelimited} 99
                      |${v3.underscoreDelimited} 99
                      |${v4.underscoreDelimited} 99
                      |${v5.underscoreDelimited} 99""".stripMargin
  
    val source = Source.fromString(csvData, Source.Formats.spaceDelimitedWithHeader)
    
    val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, marker.map(Variant.from))
    
    val toAggregatorFormat: PValueVariantRowExpr = VariantRowExpr.PValueVariantRowExpr(
        metadata = metadata,
        markerDef = markerDef,
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        nDef = Some(AnonColumnDef(LiteralColumnExpr(99))))
    
    val flipDetector = MetricTest.MockFlipDetector(
        flippedVariants = Set(v0, v3), complementedVariants = Set(v1, v3))
    
    val skipped = Set(v1, v4)
        
    val rows = source.tagFlips(markerDef, flipDetector).map(toAggregatorFormat).map { row =>
      if(skipped.contains(row.derivedFrom.originalMarker)) row.skip else row
    }
        
    val expected = SummaryStats(
      validVariants = 6 - skipped.size,
      skippedVariants = skipped.size,
      flippedVariants = 2,
      complementedVariants = 2,
      validVariantsByChromosome = zeroedChromosomeCounts ++ Seq("1" -> 1, "2" -> 2, "4" -> 1)).toFileContents.trim
      
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("stats.txt")
      
      //Read just-written data, trim to ignore off-by-last-line-ending errors we don't care about 
      val m = Metric.writeSummaryStatsTo[PValueVariantRow](file).map(_ => Files.readFrom(file).trim)
      
      doMetricTest(m, expected)(rows)
    }
  }
  
  test("writeValidVariantsTo") {
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
    
    val toAggregatorFormat: PValueVariantRowExpr = VariantRowExpr.PValueVariantRowExpr(
        metadata = metadata,
        markerDef = markerDef,
        pvalueDef = AggregatorColumnDefs.pvalue(Pvalue),
        nDef = Some(AnonColumnDef(LiteralColumnExpr(42))))
    
    val flipDetector = Helpers.FlipDetectors.NoFlipsEver
      
    val skipped = Set(Vars.y, Vars.a).map(Variant.from)
    
    val rows = source.tagFlips(markerDef, flipDetector).map(toAggregatorFormat).map { row =>
      if(skipped.contains(row.derivedFrom.marker)) row.skip else row
    }
                      
    def makeRow(variant: Variant, pvalue: Double, derivedFromRecordNumber: Option[Long]) = {
      PValueVariantRow(
          marker = variant, 
          pvalue = 99, 
          dataset = metadata.dataset,
          phenotype = metadata.phenotype,
          ancestry = metadata.ancestry,
          derivedFromRecordNumber = derivedFromRecordNumber,
          n = 42)
    }
                      
    val expected = Seq(
        makeRow(Variant(Vars.x), 99, derivedFromRecordNumber = Some(1)),
        makeRow(Variant(Vars.z), 99, derivedFromRecordNumber = Some(3)),
        makeRow(Variant(Vars.b), 99, derivedFromRecordNumber = Some(5)),
        makeRow(Variant(Vars.c), 99, derivedFromRecordNumber = Some(6)))
    
    val written: Buffer[RenderableRow] = new ArrayBuffer                       
      
    val sink: RowSink[PValueVariantRow] = new RowSink[PValueVariantRow] {
      override def accept(row: PValueVariantRow): Unit = written += row
      
      override def close(): Unit = ()
    }

    Metric.writeValidVariantsTo(sink).process(rows.records)
    
    assert(written === expected)
  }
  
  private def doMetricTest[R <: BaseVariantRow, A](
      metric: Metric[R, A], 
      expected: A)(rows: Source[VariantRow.Parsed[R]]): Unit = {
    
    assert(Fold.fold(rows.records)(metric) === expected)
    
    assert(Fold.fold(rows.records.to(List))(metric) === expected)
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
  
  private final case class MockFlipDetector(
      flippedVariants: Set[Variant] = Set.empty,
      complementedVariants: Set[Variant] = Set.empty) extends FlipDetector {
    
    override def isFlipped(variantId: Variant): Disposition = {
      val flipped = flippedVariants.contains(variantId)
      val complemented = complementedVariants.contains(variantId)
      
      (flipped, complemented) match {
        case (true, true) => Disposition.FlippedComplementStrand
        case (false, true) => Disposition.NotFlippedComplementStrand
        case (true, false) => Disposition.FlippedSameStrand
        case (false, false) => Disposition.NotFlippedSameStrand
      }
    }
  }
}
