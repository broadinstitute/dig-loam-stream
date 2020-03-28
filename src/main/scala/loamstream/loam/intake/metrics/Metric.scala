package loamstream.loam.intake.metrics

import loamstream.loam.intake.CsvRow
import cats.kernel.Monoid
import loamstream.loam.intake.ColumnName
import loamstream.loam.intake.ColumnExpr
import cats.Functor
import loamstream.loam.intake.CsvSource
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

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
    Fold.countIf[CsvRow](row => p(markerColumn.eval(row)))
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
}
