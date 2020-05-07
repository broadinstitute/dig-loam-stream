package loamstream.loam.intake.metrics

import org.scalatest.FunSuite
import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.flip.FlipDetector

/**
 * @author clint
 * May 5, 2020
 */
final class MetricTest extends FunSuite {
  import IntakeSyntax._
  
  private val Foo = ColumnName("FOO")
  private val Bar = ColumnName("BAR")
  
  private val csvData = s"""|${Foo.name} ${Bar.name}
                            |x 6
                            |y 4
                            |z 3
                            |a 5
                            |b 2
                            |c 1""".stripMargin
  
  private val source = CsvSource.fromString(csvData, CsvSource.Formats.spaceDelimitedWithHeader)
                            
  test("countGreaterThan") {
    val gt2 = Metric.countGreaterThan(Bar)(2)
    val gt4 = Metric.countGreaterThan(Bar)(4)
    
    doMetricTest(gt4, expected = 2)(source)
    doMetricTest(gt2, expected = 4)(source)
  }
  
  test("fractionGreaterThan") {
    val fracGt2 = Metric.fractionGreaterThan(Bar)(2)
    val fracGt4 = Metric.fractionGreaterThan(Bar)(4)
    
    doMetricTest(fracGt4, expected = (2d / 6d))(source)
    doMetricTest(fracGt2, expected = (4d / 6d))(source)
  }
  
  test("fractionUnknown") {
    val bioIndexClient: BioIndexClient = new MetricTest.MockBioIndexClient(knownVariants = Set("y", "z", "a"))
                      
    val fracUnknown = Metric.fractionUnknown(markerColumn = Foo, client = bioIndexClient)
   
    doMetricTest(fracUnknown, expected = (3.0 / 6.0))(source)
  }
  
  test("countKnown") {
    val bioIndexClient: BioIndexClient = new MetricTest.MockBioIndexClient(knownVariants = Set("z", "a"))
                      
    val countKnown = Metric.countKnown(markerColumn = Foo, client = bioIndexClient)
   
    doMetricTest(countKnown, expected = 2)(source)
  }
  
  test("countUnknown") {
    val bioIndexClient: BioIndexClient = new MetricTest.MockBioIndexClient(knownVariants = Set("z", "a"))
                      
    val countUnknown = Metric.countUnknown(markerColumn = Foo, client = bioIndexClient)
   
    doMetricTest(countUnknown, expected = 4)(source)
  }
  
  test("countWithDisagreeingBetaStderrZscore - no flips") {
    import _root_.loamstream.loam.intake.aggregator.ColumnNames._
    
    val csvData = s"""|${marker.name} ${beta.name} ${stderr.name} ${zscore.name}
                      |x 4 2 2
                      |y 8 2 4
                      |z 8 4 42
                      |a 5 2.5 42
                      |b 8 4 2
                      |c 9 2 4.5""".stripMargin
  
    val source = CsvSource.fromString(csvData, CsvSource.Formats.spaceDelimitedWithHeader)
    
    val flipDetector = new MetricTest.MockFlipDetector(Set.empty)
    
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore(flipDetector)()
                      
    doMetricTest(countDisagreements, expected = 2)(source)
  }
  
  test("countWithDisagreeingBetaStderrZscore - some flips") {
    import _root_.loamstream.loam.intake.aggregator.ColumnNames._
    
    val csvData = s"""|${marker.name} ${beta.name} ${stderr.name} ${zscore.name}
                      |x 4 2 2
                      |y 8 2 -4
                      |z 8 4 42
                      |a 5 2.5 42
                      |b 8 4 -2
                      |c 9 2 4.5""".stripMargin
  
    val source = CsvSource.fromString(csvData, CsvSource.Formats.spaceDelimitedWithHeader)
    
    val flipDetector = new MetricTest.MockFlipDetector(Set("y", "a", "b"))
    
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore(flipDetector)()
                      
    doMetricTest(countDisagreements, expected = 2)(source)
  }
  
  test("countWithDisagreeingBetaStderrZscore - no flips, non-default names") {
    val marker = Foo
    val beta = Bar
    val stderr = ColumnName("BAZ")
    val zscore = ColumnName("Bipp")
    
    val csvData = s"""|${marker.name} ${beta.name} ${stderr.name} ${zscore.name}
                      |x 4 2 2
                      |y 8 2 4
                      |z 8 4 42
                      |a 5 2.5 42
                      |b 8 4 2
                      |c 9 2 4.5""".stripMargin
  
    val source = CsvSource.fromString(csvData, CsvSource.Formats.spaceDelimitedWithHeader)
    
    val flipDetector = new MetricTest.MockFlipDetector(Set.empty)
    
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore(flipDetector)(
        markerColumn = marker,
        zscoreColumn = zscore,
        betaColumn = beta,
        stderrColumn = stderr)
                      
    doMetricTest(countDisagreements, expected = 2)(source)
  }
  
  test("countWithDisagreeingBetaStderrZscore - some flips, non-default names") {
    val marker = Foo
    val beta = Bar
    val stderr = ColumnName("BAZ")
    val zscore = ColumnName("Bipp")
    
    val csvData = s"""|${marker.name} ${beta.name} ${stderr.name} ${zscore.name}
                      |x 4 2 2
                      |y 8 2 -4
                      |z 8 4 42
                      |a 5 2.5 42
                      |b 8 4 -2
                      |c 9 2 4.5""".stripMargin
  
    val source = CsvSource.fromString(csvData, CsvSource.Formats.spaceDelimitedWithHeader)
    
    val flipDetector = new MetricTest.MockFlipDetector(Set("y", "a", "b"))
    
    val countDisagreements = Metric.countWithDisagreeingBetaStderrZscore(flipDetector)(
        markerColumn = marker,
        zscoreColumn = zscore,
        betaColumn = beta,
        stderrColumn = stderr)
                      
    doMetricTest(countDisagreements, expected = 2)(source)
  }
  
  private def doMetricTest[A](metric: Metric[A], expected: A)(rows: CsvSource): Unit = {
    assert(Fold.fold(rows.records)(metric) === expected)
    
    assert(Fold.fold(rows.records.toList)(metric) === expected)
  }
}

object MetricTest {
  private final class MockBioIndexClient(knownVariants: Set[String]) extends BioIndexClient {
    override def isKnown(varId: String): Boolean = knownVariants.contains(varId)
  }
  
  private final class MockFlipDetector(flippedVariants: Set[String]) extends FlipDetector {
    override def isFlipped(variantId: String): Boolean = flippedVariants.contains(variantId)
  }
}
