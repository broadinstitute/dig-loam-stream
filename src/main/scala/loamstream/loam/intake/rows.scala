package loamstream.loam.intake

import org.apache.commons.csv.CSVFormat
import scala.collection.mutable.ArrayBuffer


/**
 * @author clint
 * Dec 17, 2019
 */
trait Row {
  def values: Seq[String]
}

final case class LiteralRow(values: Seq[String]) extends Row

object LiteralRow {
  def apply(values: String*)(implicit discriminator: Int = 0): LiteralRow = new LiteralRow(values) 
}

/**
 * @author clint
 * Oct 14, 2020
 */
final case class DataRow(
  marker: Variant,
  pvalue: Double,
  zscore: Option[Double] = None,
  stderr: Option[Double] = None,
  beta: Option[Double] = None,
  oddsRatio: Option[Double] = None,
  eaf: Option[Double] = None,
  maf: Option[Double] = None,
  n: Option[Double] = None) extends Row {
  
  //NB: Profiler-informed optimization: adding to a Buffer is 2x faster than ++ or .flatten
  //We expect this method to be called a lot - once per row being output.
  override def values: Seq[String] = {
    val buffer = new ArrayBuffer[String](10) //scalastyle:ignore magic.number
    
    def add(o: Option[Double]): Unit = o match {
      case Some(d) => buffer += d.toString
      case None => ()
    }
    
    //NB: ORDERING MATTERS :\
    
    buffer += marker.underscoreDelimited
    buffer += pvalue.toString
    
    add(zscore)
    add(stderr)
    add(beta)
    add(oddsRatio)
    add(eaf)
    add(maf)
    add(n)
    
    buffer 
  }
}
