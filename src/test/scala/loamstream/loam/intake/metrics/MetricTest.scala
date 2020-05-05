package loamstream.loam.intake.metrics

import org.scalatest.FunSuite
import loamstream.loam.intake.IntakeSyntax

/**
 * @author clint
 * May 5, 2020
 */
final class MetricTest extends FunSuite {
  import IntakeSyntax._
  
  object ColumnNames {
    val Foo = ColumnName("FOO")
    val Bar = ColumnName("BAR")
  }
  
  test("countGreaterThan") {
    import ColumnNames._
    
    val csvData = s"""|${Foo.name} ${Bar.name}
                      |x 6
                      |y 4
                      |z 3
                      |a 5
                      |b 2
                      |c 1""".stripMargin
                     
    val source = CsvSource.fromString(csvData, CsvSource.Formats.spaceDelimitedWithHeader)
    
    val gt2 = Metric.countGreaterThan(Bar)(2)
    val gt4 = Metric.countGreaterThan(Bar)(4)
    
    doMetricTest(gt4, expected = 2)(source.records)
    doMetricTest(gt2, expected = 4)(source.records)
    
    doMetricTest(gt4, expected = 2)(source.records.toList)
    doMetricTest(gt2, expected = 4)(source.records.toList)
  }
  
  test("fractionGreaterThan") {
    import ColumnNames._
    
    val csvData = s"""|${Foo.name} ${Bar.name}
                      |x 6
                      |y 4
                      |z 3
                      |a 5
                      |b 2
                      |c 1""".stripMargin
                     
    val source = CsvSource.fromString(csvData, CsvSource.Formats.spaceDelimitedWithHeader)
    
    val fracGt2 = Metric.fractionGreaterThan(Bar)(2)
    val fracGt4 = Metric.fractionGreaterThan(Bar)(4)
    
    doMetricTest(fracGt4, expected = (2d / 6d))(source.records)
    doMetricTest(fracGt2, expected = (4d / 6d))(source.records)
    
    doMetricTest(fracGt4, expected = (2d / 6d))(source.records.toList)
    doMetricTest(fracGt2, expected = (4d / 6d))(source.records.toList)
  }
  
  private def doMetricTest[A](metric: Metric[A], expected: A)(rows: TraversableOnce[CsvRow]): Unit = {
    val actual = Fold.fold(rows)(metric)
    
    assert(actual === expected)
  }
}
