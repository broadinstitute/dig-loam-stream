package loamstream.loam.intake.metrics

import _root_.loamstream.loam.intake.ColumnName
import _root_.loamstream.loam.intake.CsvRow

import _root_.loamstream.loam.intake.aggregator
import _root_.loamstream.loam.intake.flip.FlipDetector
import _root_.loamstream.loam.intake.ColumnExpr


/**
 * @author clint
 * Mar 27, 2020
 */
object Metric {
  private val functor = Fold.foldFunctor[CsvRow]
  
  def countGreaterThan(column: ColumnName)(threshold: Double): Metric[Int] = {
    val columnAsDouble = column.asDouble
    
    Fold.countIf[CsvRow](row => columnAsDouble.eval(row) > threshold)
  }
  
  def fractionGreaterThan(column: ColumnName)(threshold: Double): Metric[Double] = {
    val countGt: Metric[Int] = countGreaterThan(column)(threshold)

    toFraction(countGt, Fold.count)
  }
  
  def countKnown(markerColumn: ColumnName, client: BioIndexClient): Metric[Int] = {
    countKnownOrUnknown(markerColumn, client)(client.isKnown)
  }
  
  def countUnknown(markerColumn: ColumnName, client: BioIndexClient): Metric[Int] = {
    countKnownOrUnknown(markerColumn, client)(client.isUnknown)
  }
  
  def countKnownOrUnknown(markerColumn: ColumnName, client: BioIndexClient)(p: String => Boolean): Metric[Int] = {
    Fold.countIf(row => p(markerColumn.eval(row)))
  }
  
  
  def fractionUnknown(markerColumn: ColumnName, client: BioIndexClient): Metric[Double] = {
    toFraction(countUnknown(markerColumn, client), Fold.count)
  }
  
  def toFraction[A](
      numeratorMetric: Metric[A], 
      denominatorMetric: Metric[A])(implicit ev: Numeric[A]): Metric[Double] = {
    
    //TODO: Figure out what I have to import to be able to just say .map()
    //instead of making a functor and working through that.
    functor.fmap(Fold.combine(numeratorMetric, denominatorMetric)) { 
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
        val agreesFn: (Double, Double, Double) => Boolean = if(isFlipped) agreesFlip else agreesNoFlip
        
        agreesFn(z, beta, se)
      }
    }
    
    def disagrees(row: CsvRow): Boolean = !agreesExpr.eval(row)
    
    Fold.countIf(disagrees)
  }
}
