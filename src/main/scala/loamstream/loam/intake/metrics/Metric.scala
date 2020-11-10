package loamstream.loam.intake.metrics

import loamstream.loam.intake.Variant
import loamstream.loam.intake.DataRow
import loamstream.loam.intake.flip.FlipDetector
import loamstream.util.Fold
import loamstream.loam.intake.RowTuple
import loamstream.loam.intake.CsvRow


/**
 * @author clint
 * Mar 27, 2020
 */
object Metric {
  def countGreaterThan(column: RowTuple => Double)(threshold: Double): Metric[Int] = {
    Fold.countIf[RowTuple](row => column(row) > threshold)
  }
  
  def fractionGreaterThan(column: RowTuple => Double)(threshold: Double): Metric[Double] = {
    fractionOfTotal(countGreaterThan(column)(threshold))
  }
  
  def countKnown(client: BioIndexClient): Metric[Int] = {
    countKnownOrUnknown(client)(client.isKnown)
  }
  
  def countUnknown(client: BioIndexClient): Metric[Int] = {
    countKnownOrUnknown(client)(client.isUnknown)
  }
  
  private def countKnownOrUnknown(client: BioIndexClient)(p: Variant => Boolean): Metric[Int] = {
    Fold.countIf { case (_, dataRow) => p(dataRow.marker) }
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
  
  def countWithDisagreeingBetaStderrZscore(epsilon: Double = 1e-8d): Metric[Int] = {
    //z = beta / se  or  -(beta / se) if flipped
    
    def agrees(expected: Double, actual: Double): Boolean = scala.math.abs(expected - actual) < epsilon
    
    def agreesNoFlip(z: Double, beta: Double, se: Double): Boolean = agrees(z, beta / se)
    
    def agreesFlip(z: Double, beta: Double, se: Double): Boolean = agrees(z, -(beta / se))
    
    def isFlipped(sourceRow: CsvRow.WithFlipTag): Boolean = sourceRow.disposition.isFlipped
    
    val agreesFn: RowTuple => Boolean = { 
      case (sourceRow, DataRow(marker, _, Some(z), Some(se), Some(beta), _, _, _, _)) => {
        if(isFlipped(sourceRow)) {
          agrees(z, -(beta / se))
        } else {
          agrees(z, beta / se)
        }
      }
      case _ => false
    }
    
    def disagrees(row: RowTuple): Boolean = !agreesFn(row)
    
    Fold.countIf(disagrees)
  }
  
  def fractionWithDisagreeingBetaStderrZscore(epsilon: Double = 1e-8d): Metric[Double] = {
    fractionOfTotal(countWithDisagreeingBetaStderrZscore(epsilon))
  }
  
  def mean[N](column: RowTuple => N)(implicit ev: Numeric[N]): Metric[Double] = {
    val sumFold: Fold[RowTuple, N, N] = Fold.sum(column)
    val countFold: Fold[RowTuple, Int, Int] = Fold.count
    
    Fold.combine(sumFold, countFold).map {
      case (sum, count) => ev.toDouble(sum) / count.toDouble
    }
  }
}
