package loamstream.loam.intake.metrics

import loamstream.loam.intake.ColumnExpr
import loamstream.loam.intake.ColumnName
import loamstream.loam.intake.CsvRow
import loamstream.loam.intake.aggregator
import loamstream.loam.intake.flip.FlipDetector
import loamstream.util.Fold
import loamstream.loam.intake.aggregator.DataRow


/**
 * @author clint
 * Mar 27, 2020
 */
object Metric {
  def countGreaterThan(column: DataRow => Double)(threshold: Double): Metric[Int] = {
    Fold.countIf[aggregator.DataRow](row => column(row) > threshold)
  }
  
  def fractionGreaterThan(column: DataRow => Double)(threshold: Double): Metric[Double] = {
    fractionOfTotal(countGreaterThan(column)(threshold))
  }
  
  def countKnown(client: BioIndexClient): Metric[Int] = {
    countKnownOrUnknown(client)(client.isKnown)
  }
  
  def countUnknown(client: BioIndexClient): Metric[Int] = {
    countKnownOrUnknown(client)(client.isUnknown)
  }
  
  private def countKnownOrUnknown(client: BioIndexClient)(p: String => Boolean): Metric[Int] = {
    Fold.countIf(row => p(row.marker))
  }
  
  def fractionUnknown(client: BioIndexClient): Metric[Double] = {
    fractionOfTotal(countUnknown(client))
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
      flipDetector: FlipDetector,
      epsilon: Double = 1e-8d): Metric[Int] = {
    
    //z = beta / se  or  -(beta / se) if flipped
    
    def agrees(expected: Double, actual: Double): Boolean = scala.math.abs(expected - actual) < epsilon
    
    def agreesNoFlip(z: Double, beta: Double, se: Double): Boolean = agrees(z, beta / se)
    
    def agreesFlip(z: Double, beta: Double, se: Double): Boolean = agrees(z, -(beta / se))
    
    def isFlipped(marker: String): Boolean = flipDetector.isFlipped(marker).isFlipped
    
    val agreesFn: DataRow => Boolean = { 
      case DataRow(marker, _, Some(z), Some(se), Some(beta), _, _, _, _) => {
        if(isFlipped(marker)) {
          agrees(z, -(beta / se))
        } else {
          agrees(z, beta / se)
        }
      }
      case _ => false
    }
    
    def disagrees(row: DataRow): Boolean = !agreesFn(row)
    
    Fold.countIf(disagrees)
  }
  
  def fractionWithDisagreeingBetaStderrZscore(
      flipDetector: FlipDetector, 
      epsilon: Double = 1e-8d): Metric[Double] = {
    
    fractionOfTotal(countWithDisagreeingBetaStderrZscore(flipDetector, epsilon))
  }
  
  def mean[N](column: DataRow => N)(implicit ev: Numeric[N]): Metric[Double] = {
    val sumFold: Fold[DataRow, N, N] = Fold.sum(column)
    val countFold: Fold[DataRow, Int, Int] = Fold.count
    
    Fold.combine(sumFold, countFold).map {
      case (sum, count) => ev.toDouble(sum) / count.toDouble
    }
  }
}
