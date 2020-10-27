package loamstream.loam.intake.aggregator

import loamstream.loam.intake.ColumnExpr
import loamstream.loam.intake.LiteralRow
import loamstream.loam.intake.Row
import loamstream.util.TimeUtils
import scala.collection.mutable.ArrayBuffer

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
  
  //NB: Profiler-informed optimization: adding to a Buffer is 2x faster than ++ or .flatten
  override def values: Seq[String] = {
    val buffer = new ArrayBuffer[String](10)
    
    def add(o: Option[Double]): Unit = o match {
      case Some(d) => buffer += d.toString
      case None => ()
    }
    
    buffer += marker
    buffer += pvalue.toString
    
    add(zscore)
    add(stderr)
    add(beta)
    add(oddsRatio)
    add(eaf)
    add(maf)
    add(n)
    
    buffer //TODO: ORDERING
  }
}
