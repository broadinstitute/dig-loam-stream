package loamstream.loam.intake.metrics

import _root_.loamstream.loam.intake.ColumnName
import _root_.loamstream.loam.intake.CsvRow

import _root_.loamstream.loam.intake.aggregator
import _root_.loamstream.loam.intake.flip.FlipDetector
import _root_.loamstream.loam.intake.ColumnExpr
import com.google.common.collect.TopKSelector
import com.google.common.collect.TopKSelector
import com.google.common.collect.TopKSelector
import java.util.Comparator


/**
 * @author clint
 * Mar 27, 2020
 */
object Metric {
  def countGreaterThan(column: ColumnName)(threshold: Double): Metric[Int] = {
    val columnAsDouble = column.asDouble
    
    Fold.countIf[CsvRow](row => columnAsDouble.eval(row) > threshold)
  }
  
  def fractionGreaterThan(column: ColumnName)(threshold: Double): Metric[Double] = {
    fractionOfTotal(countGreaterThan(column)(threshold))
  }
  
  def countKnown(markerColumn: ColumnName, client: BioIndexClient): Metric[Int] = {
    countKnownOrUnknown(markerColumn, client)(client.isKnown)
  }
  
  def countUnknown(markerColumn: ColumnName, client: BioIndexClient): Metric[Int] = {
    countKnownOrUnknown(markerColumn, client)(client.isUnknown)
  }
  
  private def countKnownOrUnknown(markerColumn: ColumnName, client: BioIndexClient)(p: String => Boolean): Metric[Int] = {
    Fold.countIf(row => p(markerColumn.eval(row)))
  }
  
  def fractionUnknown(markerColumn: ColumnName, client: BioIndexClient): Metric[Double] = {
    fractionOfTotal(countUnknown(markerColumn, client))
  }
  
  private def fractionOfTotal[A](numeratorMetric: Metric[A])(implicit ev: Numeric[A]): Metric[Double] = {
    toFraction(numeratorMetric, Fold.count.map(ev.fromInt))
  }
  
  private def toFraction[A](
      numeratorMetric: Metric[A], 
      denominatorMetric: Metric[A])(implicit ev: Numeric[A]): Metric[Double] = {
    
    Fold.combine(numeratorMetric, denominatorMetric).map {
      case (numerator, denominator) => ev.toDouble(numerator) / ev.toDouble(denominator)
    }
  }
  
  def countWithDisagreeingBetaStderrZscore(
      flipDetector: FlipDetector)(
      markerColumn: ColumnName = aggregator.ColumnNames.marker,
      zscoreColumn: ColumnName = aggregator.ColumnNames.zscore,
      betaColumn: ColumnName = aggregator.ColumnNames.beta,
      stderrColumn: ColumnName = aggregator.ColumnNames.stderr,
      epsilon: Double = 1e-8d): Metric[Int] = {
    
    //z = beta / se  or  -(beta / se) if flipped
    
    def agrees(expected: Double, actual: Double): Boolean = scala.math.abs(expected - actual) < epsilon
    
    def agreesNoFlip(z: Double, beta: Double, se: Double): Boolean = agrees(z, beta / se)
    
    def agreesFlip(z: Double, beta: Double, se: Double): Boolean = agrees(z, -(beta / se))
    
    val isFlippedExpr: ColumnExpr[Boolean] = markerColumn.map(flipDetector.isFlipped)
      
    val agreesExpr: ColumnExpr[Boolean] = {
      for {
        isFlipped <- isFlippedExpr
        z <- zscoreColumn.asDouble
        se <- stderrColumn.asDouble
        beta <- betaColumn.asDouble
      } yield {
        if(isFlipped) {
          agrees(z, -(beta / se))
        } else {
          agrees(z, beta / se)
        }
      }
    }
    
    def disagrees(row: CsvRow): Boolean = !agreesExpr.eval(row)
    
    Fold.countIf(disagrees)
  }
  
  def mean(columnExpr: ColumnExpr[_]): Metric[Double] = {
    val columnAsDouble = columnExpr.asString.asDouble
    
    val sumFold: Fold[CsvRow, Double, Double] = Fold(0.0, _ + columnAsDouble.eval(_), identity)
    val countFold: Fold[CsvRow, Int, Int] = Fold.count
    
    Fold.combine(sumFold, countFold).map {
      case (sum, count) => sum / count.toDouble
    }
  }
}
