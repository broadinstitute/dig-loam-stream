package loamstream.loam.intake

import org.apache.commons.csv.CSVFormat
import scala.collection.mutable.ArrayBuffer
import loamstream.loam.intake.flip.Disposition
import scala.util.Failure
import scala.util.Try
import org.json4s.JsonAST.JValue
import org.json4s.JsonAST.JNull
import org.json4s.JsonAST.JNothing


/**
 * @author clint
 * Dec 17, 2019
 */
trait RenderableRow extends HasHeaders with HasValues

trait RenderableJsonRow extends RenderableRow {
  def jsonValues: Seq[(String, JValue)]
  
  override def values: Seq[Option[String]] = {
    import org.json4s.jackson.JsonMethods._
    
    jsonValues.collect { 
      case (_, JNull | JNothing) => None
      case (_, jv) =>Option(compact(render(jv))) 
    }
  }

  override def headers: Seq[String] = jsonValues.collect { case (k, _) => k }
}



final case class LiteralRow(values: Seq[Option[String]], headers: Seq[String] = Nil) extends RenderableRow

object LiteralRow {
  def apply(values: String*): LiteralRow = new LiteralRow(values.map(Some(_)))
  
  def apply(headers: Seq[String], values: Seq[String])(implicit discriminator: DummyImplicit): LiteralRow = {
    new LiteralRow(values = values.map(Some(_)), headers = headers)
  }
}

trait SkippableRow[A <: SkippableRow[A]] { self: A =>
  def isSkipped: Boolean
  
  final def notSkipped: Boolean = !isSkipped
  
  def skip: A
}

trait HasValues {
  def values: Seq[Option[String]]
}

trait HasHeaders {
  def headers: Seq[String]
}

trait RowWithSize {
  def size: Int
  
  final def hasIndex(i: Int): Boolean = i > 0 && i < size
}

trait RowWithRecordNumber {
  def recordNumber: Long
}

trait KeyedRow extends RowWithSize {
  def headers: Seq[String]
  
  def hasField(name: String): Boolean 
  
  def getFieldByName(name: String): String
  
  def getFieldByNameOpt(name: String): Option[String] = Try(getFieldByName(name)).toOption
}

trait IndexedRow extends RowWithSize with RenderableRow {
  def getFieldByIndex(i: Int): String
  
  def values: Seq[Option[String]] = (0 until size).map(getFieldByIndex).map(Option(_))
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
  
  import AggregatorVariantRow.fieldCount
  
  override def headers: Seq[String] = {
    val buffer = new ArrayBuffer[String](fieldCount) //scalastyle:ignore magic.number
    
    def add(o: Option[_], columnName: ColumnName): Unit = {
      if(o.isDefined) { 
        buffer += columnName.name
      }
    }
    
    //TODO: This will be wrong is non-default column names were used :( :( 
    
    buffer += AggregatorColumnNames.marker.name
    buffer += AggregatorColumnNames.pvalue.name
    
    add(zscore, AggregatorColumnNames.zscore)
    add(stderr, AggregatorColumnNames.stderr)
    add(beta, AggregatorColumnNames.beta)
    add(oddsRatio, AggregatorColumnNames.odds_ratio)
    add(eaf, AggregatorColumnNames.eaf)
    add(maf, AggregatorColumnNames.maf)
    add(n, AggregatorColumnNames.n)
    
    buffer
  }
  
  //NB: Profiler-informed optimization: adding to a Buffer is 2x faster than ++ or .flatten
  //We expect this method to be called a lot - once per row being output.
  override def values: Seq[Option[String]] = {
    val buffer = new ArrayBuffer[Option[String]](fieldCount) //scalastyle:ignore magic.number
    
    def add(o: Option[Double]): Unit = buffer += (o.map(_.toString))
    
    //NB: ORDERING MATTERS :\
    
    buffer += Some(marker.underscoreDelimited)
    buffer += Some(pvalue.toString)
    
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

object AggregatorVariantRow {
  private val fieldCount: Int = 10 //scalastyle:ignore magic.number
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
    
    override def headers: Seq[String] = delegate.headers
    
    override def hasField(name: String): Boolean = delegate.hasField(name)
    
    override def getFieldByName(name: String): String = delegate.getFieldByName(name)
    
    override def getFieldByIndex(i: Int): String = delegate.getFieldByIndex(i)
    
    override def size: Int = delegate.size
    
    override def recordNumber: Long = delegate.recordNumber
  }
  
  sealed trait Parsed extends DataRow {
    val derivedFrom: Tagged
    
    def aggRowOpt: Option[AggregatorVariantRow]
    
    override def skip: Parsed
    
    def transform(f: AggregatorVariantRow => AggregatorVariantRow): Parsed
    
    final def isFlipped: Boolean = derivedFrom.isFlipped
    final def notFlipped: Boolean = derivedFrom.notFlipped
    
    final def isSameStrand: Boolean = derivedFrom.isSameStrand
    final def isComplementStrand: Boolean = derivedFrom.isComplementStrand
    
    override def headers: Seq[String] = derivedFrom.headers
    
    override def hasField(name: String): Boolean = derivedFrom.hasField(name)
    
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
