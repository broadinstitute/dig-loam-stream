package loamstream.loam.intake

import org.apache.commons.csv.CSVFormat
import scala.collection.mutable.ArrayBuffer
import loamstream.loam.intake.flip.Disposition
import scala.util.Failure
import scala.util.Try


/**
 * @author clint
 * Dec 17, 2019
 */
trait RenderableRow {
  def values: Seq[String]
}

final case class LiteralRow(values: Seq[String]) extends RenderableRow

object LiteralRow {
  def apply(values: String*)(implicit discriminator: Int = 0): LiteralRow = new LiteralRow(values) 
}

trait SkippableRow[A <: SkippableRow[A]] { self: A =>
  def isSkipped: Boolean
  
  final def notSkipped: Boolean = !isSkipped
  
  def skip: A
}

trait RowWithSize {
  def size: Int
}

trait RowWithRecordNumber {
  def recordNumber: Long
}

trait KeyedRow extends RowWithSize {
  def getFieldByName(name: String): String
  
  def getFieldByNameOpt(name: String): Option[String] = Try(getFieldByName(name)).toOption
}

trait IndexedRow extends RowWithSize with RenderableRow {
  def getFieldByIndex(i: Int): String
  
  def values: Seq[String] = (0 until size).map(getFieldByIndex)
}

/**
 * @author clint
 * Oct 14, 2020
 */
final case class AggregatorVariantRow(
  marker: Variant,
  pvalue: Double,
  zscore: Option[Double] = None,
  stderr: Option[Double] = None,
  beta: Option[Double] = None,
  oddsRatio: Option[Double] = None,
  eaf: Option[Double] = None,
  maf: Option[Double] = None,
  n: Option[Double] = None,
  derivedFromRecordNumber: Option[Long] = None) extends RenderableRow {
  
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

/**
 * @author clint
 * Dec 1, 2020
 */
object VariantRow {
  final case class Tagged(
      delegate: DataRow,
      marker: Variant,
      originalMarker: Variant,
      disposition: Disposition,
      isSkipped: Boolean = false) extends DataRow {
    
    override def skip: Tagged = copy(isSkipped = true)
    
    def isFlipped: Boolean = disposition.isFlipped
    def notFlipped: Boolean = disposition.notFlipped
    
    def isSameStrand: Boolean = disposition.isSameStrand
    def isComplementStrand: Boolean = disposition.isComplementStrand
    
    override def getFieldByName(name: String): String = delegate.getFieldByName(name)
    
    override def getFieldByIndex(i: Int): String = delegate.getFieldByIndex(i)
    
    override def size: Int = delegate.size
    
    override def recordNumber: Long = delegate.recordNumber
  }
  
  sealed trait Parsed extends DataRow {
    val derivedFrom: Tagged
    
    def aggRowOpt: Option[AggregatorVariantRow]
    
    def isSkipped: Boolean
    
    override def skip: Parsed
    
    def transform(f: AggregatorVariantRow => AggregatorVariantRow): Parsed
    
    final def isFlipped: Boolean = derivedFrom.isFlipped
    final def notFlipped: Boolean = derivedFrom.notFlipped
    
    final def isSameStrand: Boolean = derivedFrom.isSameStrand
    final def isComplementStrand: Boolean = derivedFrom.isComplementStrand
    
    override def getFieldByName(name: String): String = derivedFrom.getFieldByName(name)
    
    override def getFieldByIndex(i: Int): String = derivedFrom.getFieldByIndex(i)
    
    override def size: Int = derivedFrom.size
    
    override def recordNumber: Long = derivedFrom.recordNumber
  }
  
  final case class Transformed(
      derivedFrom: Tagged,
      aggRow: AggregatorVariantRow) extends Parsed {
    
    override def aggRowOpt: Option[AggregatorVariantRow] = Some(aggRow)
    
    override def isSkipped: Boolean = false
    
    override def skip: Skipped = Skipped(derivedFrom, aggRowOpt)
    
    override def transform(f: AggregatorVariantRow => AggregatorVariantRow): Transformed = copy(aggRow = f(aggRow))
  }
  
  final case class Skipped(
      derivedFrom: Tagged, 
      aggRowOpt: Option[AggregatorVariantRow],
      message: Option[String] = None,
      cause: Option[Failure[Parsed]] = None) extends Parsed {
    
    override def isSkipped: Boolean = true
    
    override def skip: Skipped = this
    
    override def transform(f: AggregatorVariantRow => AggregatorVariantRow): Skipped = this
  }
}
