package loamstream.loam.intake.aggregator

import loamstream.loam.intake.ColumnExpr
import loamstream.loam.intake.LiteralRow
import loamstream.loam.intake.Row

/**
 * @author clint
 * Oct 14, 2020
 */
final case class DataRow(
  marker: String,
  pvalue: Double,
  zscore: Option[Double] = None,
  stderr: Option[Double] = None,
  beta: Option[Double] = None,
  oddsRatio: Option[Double] = None,
  eaf: Option[Double] = None,
  maf: Option[Double] = None,
  n: Option[Double] = None) extends Row {
  
  override def values: Seq[String] = ???
}
