package loamstream.loam.intake

import org.apache.commons.csv.CSVFormat
import scala.collection.mutable.ArrayBuffer
import loamstream.loam.intake.flip.Disposition
import scala.util.Failure
import scala.util.Try
import org.json4s.JsonAST._


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

sealed trait BaseVariantRow extends RenderableJsonRow { 
  def marker: Variant 
  
  def dataset: String
  
  def phenotype: String
  
  def ancestry: Ancestry
  
  def derivedFromRecordNumber: Option[Long]
  
  protected def commonJson: Seq[(String, JValue)] = BaseVariantRow.commonJson(this)
}

object BaseVariantRow {
  val fieldCount: Int = 18 //scalastyle:ignore magic.number

  def toJson(o: Option[_]): JValue = o match {
    case Some(d: Double) => JDouble(d)
    case Some(l: Long) => JLong(l)
    case Some(s: String) => JString(s)
    case _ => JNull
  }
  
  def commonJson(row: BaseVariantRow): Seq[(String, JValue)] = {
    import row._
    
    Seq(
      AggregatorJsonKeys.varId -> JString(marker.colonDelimited),
      AggregatorJsonKeys.chromosome -> JString(marker.chrom), 
      AggregatorJsonKeys.position -> JLong(marker.pos),
      AggregatorJsonKeys.reference -> JString(marker.ref),
      AggregatorJsonKeys.alt -> JString(marker.alt),
      AggregatorJsonKeys.multiAllelic -> JBool(marker.isMultiAllelic),
      AggregatorJsonKeys.dataset -> JString(dataset),
      AggregatorJsonKeys.phenotype -> JString(phenotype),
      AggregatorJsonKeys.ancestry -> JString(ancestry.name))
  }
  
  object Headers {
    val forVariantData: Seq[String] = Seq(
      AggregatorColumnNames.marker,
      AggregatorColumnNames.pvalue,
      
      AggregatorColumnNames.zscore,
      AggregatorColumnNames.stderr,
      AggregatorColumnNames.beta,
      AggregatorColumnNames.odds_ratio,
      AggregatorColumnNames.eaf,
      AggregatorColumnNames.maf,
      AggregatorColumnNames.n).map(_.name)
      
    //TODO: FIXME
    val forVariantCountData: Seq[String] = Seq(
      AggregatorColumnNames.marker.name,
      AggregatorJsonKeys.alleleCount,
      AggregatorJsonKeys.alleleCountCases, 
      AggregatorJsonKeys.alleleCountControls,
      AggregatorJsonKeys.heterozygousCases, 
      AggregatorJsonKeys.heterozygousControls, 
      AggregatorJsonKeys.homozygousCases, 
      AggregatorJsonKeys.homozygousControls)
  }
}

/**
 * @author clint
 * Oct 14, 2020
 */
final case class VariantCountRow(
  marker: Variant,
  dataset: String,
  phenotype: String,
  ancestry: Ancestry,
  alleleCount: Option[Long],
  alleleCountCases: Option[Long], 
  alleleCountControls: Option[Long],
  heterozygousCases: Option[Long], 
  heterozygousControls: Option[Long], 
  homozygousCases: Option[Long], 
  homozygousControls: Option[Long], 
  derivedFromRecordNumber: Option[Long] = None) extends BaseVariantRow {
  
  override def jsonValues: Seq[(String, JValue)] = VariantCountRow.JsonSerializer(this)
  
  override def headers: Seq[String] = BaseVariantRow.Headers.forVariantCountData
  
  //NB: Profiler-informed optimization: adding to a Buffer is 2x faster than ++ or .flatten
  //We expect this method to be called a lot - once per row being output.
  override def values: Seq[Option[String]] = {
    
    val buffer = new ArrayBuffer[Option[String]](BaseVariantRow.fieldCount) 
    
    def add(d: Double): Unit = buffer += Some(d.toString)
    
    def addOpt(o: Option[_]): Unit = buffer += (o.map(_.toString))
    
    //NB: ORDERING MATTERS :\
    
    buffer += Some(marker.underscoreDelimited)
    
    addOpt(alleleCount)
    addOpt(alleleCountCases) 
    addOpt(alleleCountControls)
    addOpt(heterozygousCases) 
    addOpt(heterozygousControls) 
    addOpt(homozygousCases) 
    addOpt(homozygousControls) 

    buffer 
  }
}

object VariantCountRow {
  implicit object JsonSerializer extends Serializer[VariantCountRow, Seq[(String, JValue)]] {
    override def apply(row: VariantCountRow): Seq[(String, JValue)] = {
      import row._
      import BaseVariantRow.toJson
      
      BaseVariantRow.commonJson(row) ++
      Seq(
        AggregatorJsonKeys.alleleCount -> toJson(alleleCount),
        AggregatorJsonKeys.alleleCountCases -> toJson(alleleCountCases), 
        AggregatorJsonKeys.alleleCountControls -> toJson(alleleCountControls),
        AggregatorJsonKeys.heterozygousCases -> toJson(heterozygousCases), 
        AggregatorJsonKeys.heterozygousControls -> toJson(heterozygousControls), 
        AggregatorJsonKeys.homozygousCases -> toJson(homozygousCases), 
        AggregatorJsonKeys.homozygousControls -> toJson(homozygousControls) 
      )
    }
  }
}

/**
 * @author clint
 * Oct 14, 2020
 */
final case class PValueVariantRow(
  marker: Variant,
  pvalue: Double,
  dataset: String,
  phenotype: String,
  ancestry: Ancestry,
  zscore: Option[Double] = None,
  stderr: Option[Double] = None,
  beta: Option[Double] = None,
  oddsRatio: Option[Double] = None,
  eaf: Option[Double] = None,  
  maf: Option[Double] = None,
  n: Double,
  derivedFromRecordNumber: Option[Long] = None) extends BaseVariantRow {
  
  override def jsonValues: Seq[(String, JValue)] = PValueVariantRow.JsonSerializer(this)
  
  //TODO: This will be wrong if non-default column names were used :( :(
  override def headers: Seq[String] = BaseVariantRow.Headers.forVariantData
  
  //NB: Profiler-informed optimization: adding to a Buffer is 2x faster than ++ or .flatten
  //We expect this method to be called a lot - once per row being output.
  override def values: Seq[Option[String]] = {
    
    val buffer = new ArrayBuffer[Option[String]](BaseVariantRow.fieldCount) 
    
    def add(d: Double): Unit = buffer += Some(d.toString)
    
    def addOpt(o: Option[_]): Unit = buffer += (o.map(_.toString))
    
    //NB: ORDERING MATTERS :\
    
    buffer += Some(marker.underscoreDelimited)
    buffer += Some(pvalue.toString)
    
    addOpt(zscore)
    addOpt(stderr)
    addOpt(beta)
    addOpt(oddsRatio)
    addOpt(eaf)
    addOpt(maf)
    add(n)

    buffer 
  }
}

object PValueVariantRow {
  implicit object JsonSerializer extends Serializer[PValueVariantRow, Seq[(String, JValue)]] {
    override def apply(row: PValueVariantRow): Seq[(String, JValue)] = {
      import row._
      import BaseVariantRow.toJson
      
      BaseVariantRow.commonJson(row) ++
      Seq(
        AggregatorJsonKeys.pValue -> JString(pvalue.toString),
        AggregatorJsonKeys.beta -> toJson(beta),
        AggregatorJsonKeys.oddsRatio -> toJson(oddsRatio),
        AggregatorJsonKeys.eaf -> toJson(eaf),
        AggregatorJsonKeys.maf -> toJson(maf),
        AggregatorJsonKeys.stdErr -> toJson(stderr),
        AggregatorJsonKeys.zScore -> toJson(zscore),
        AggregatorJsonKeys.n -> JDouble(n) 
      )
    }
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
    
    override def headers: Seq[String] = delegate.headers
    
    override def hasField(name: String): Boolean = delegate.hasField(name)
    
    override def getFieldByName(name: String): String = delegate.getFieldByName(name)
    
    override def getFieldByIndex(i: Int): String = delegate.getFieldByIndex(i)
    
    override def size: Int = delegate.size
    
    override def recordNumber: Long = delegate.recordNumber
  }
  
  sealed trait Parsed[R <: BaseVariantRow] extends DataRow {
    val derivedFrom: Tagged
    
    def aggRowOpt: Option[R]
    
    override def skip: Parsed[R]
    
    def transform(f: R => R): Parsed[R]
    
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
  
  final case class Transformed[R <: BaseVariantRow](
      derivedFrom: Tagged,
      aggRow: R) extends Parsed[R] {
    
    override def aggRowOpt: Option[R] = Some(aggRow)
    
    override def isSkipped: Boolean = false
    
    override def skip: Skipped[R] = Skipped(derivedFrom, aggRowOpt)
    
    override def transform(f: R => R): Transformed[R] = copy(aggRow = f(aggRow))
  }
  
  final case class Skipped[R <: BaseVariantRow](
      derivedFrom: Tagged, 
      aggRowOpt: Option[R],
      message: Option[String] = None,
      cause: Option[Failure[Parsed[R]]] = None) extends Parsed[R] {
    
    override def isSkipped: Boolean = true
    
    override def skip: Skipped[R] = this
    
    override def transform(f: R => R): Skipped[R] = this
  }
}
