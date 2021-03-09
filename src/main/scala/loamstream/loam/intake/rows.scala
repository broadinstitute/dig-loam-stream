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

/**
 * @author clint
 * Oct 14, 2020
 */
final case class AggregatorVariantRow(
  marker: Variant,
  pvalue: Double,
  dataset: String,
  phenotype: String,
  ancestry: Ancestry,
  zscore: Option[Double] = None,
  stderr: Option[Double] = None,
  beta: Option[Double] = None,
  oddsRatio: Option[Double] = None,
  n: Option[Double] = None,
  eaf: Option[Double] = None,  
  maf: Option[Double] = None,
  mafCasesControls: Option[Long] = None,
  alleleCount: Option[Long] = None,
  alleleCountCases: Option[Long] = None, 
  alleleCountControls: Option[Long] = None,
  heterozygousCountCases: Option[Long] = None, 
  heterozygousCountControls: Option[Long] = None, 
  homozygousCountCases: Option[Long] = None, 
  homozygousCountControls: Option[Long] = None, 
  derivedFromRecordNumber: Option[Long] = None) extends RenderableJsonRow {
  
  override def jsonValues: Seq[(String, JValue)] = {
    def toJson(o: Option[_]): JValue = o match {
      case Some(d: Double) => JDouble(d)
      case Some(l: Long) => JLong(l)
      case Some(s: String) => JString(s)
      case _ => JNull
    }

    //TODO: Something faster?  Don't make an array of parts and throw it away?
    def multiAllelic: Boolean = marker.alt.split(',').size > 1

    Seq(
      AggregatorJsonKeys.varId -> JString(marker.underscoreDelimited),
      AggregatorJsonKeys.chromosome -> JString(marker.chrom), 
      AggregatorJsonKeys.position -> JLong(marker.pos),
      AggregatorJsonKeys.reference -> JString(marker.ref),
      AggregatorJsonKeys.alt -> JString(marker.alt),
      AggregatorJsonKeys.multiAllelic -> JBool(multiAllelic),
      AggregatorJsonKeys.dataset -> JString(dataset),
      AggregatorJsonKeys.phenotype -> JString(phenotype),
      AggregatorJsonKeys.ancestry -> JString(ancestry.name),
      AggregatorJsonKeys.pValue -> JString(pvalue.toString),
      AggregatorJsonKeys.beta -> toJson(beta),
      AggregatorJsonKeys.oddsRatio -> toJson(oddsRatio),
      AggregatorJsonKeys.eaf -> toJson(eaf),
      AggregatorJsonKeys.maf -> toJson(maf),
      AggregatorJsonKeys.stdErr -> toJson(stderr),
      AggregatorJsonKeys.zScore -> toJson(zscore),
      AggregatorJsonKeys.n -> toJson(n),
      AggregatorJsonKeys.mafCasesControls -> toJson(mafCasesControls),
      AggregatorJsonKeys.alleleCount -> toJson(alleleCount),
      AggregatorJsonKeys.alleleCountCases -> toJson(alleleCountCases), 
      AggregatorJsonKeys.alleleCountControls -> toJson(alleleCountControls),
      AggregatorJsonKeys.heterozygousCountCases -> toJson(heterozygousCountCases), 
      AggregatorJsonKeys.heterozygousCountControls -> toJson(heterozygousCountControls), 
      AggregatorJsonKeys.homozygousCountCases -> toJson(homozygousCountCases), 
      AggregatorJsonKeys.homozygousCountControls -> toJson(homozygousCountControls), 
    )
  }
  
  //TODO: This will be wrong if non-default column names were used :( :(
  override def headers: Seq[String] = AggregatorVariantRow.defaultHeaders
  
  //NB: Profiler-informed optimization: adding to a Buffer is 2x faster than ++ or .flatten
  //We expect this method to be called a lot - once per row being output.
  override def values: Seq[Option[String]] = {
    import AggregatorVariantRow.fieldCount
    
    val buffer = new ArrayBuffer[Option[String]](fieldCount) //scalastyle:ignore magic.number
    
    def add(o: Option[_]): Unit = buffer += (o.map(_.toString))
    
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
   
    add(mafCasesControls)
    add(alleleCount)
    add(alleleCountCases) 
    add(alleleCountControls)
    add(heterozygousCountCases) 
    add(heterozygousCountControls) 
    add(homozygousCountCases) 
    add(homozygousCountControls) 
    
    buffer 
  }
}

object AggregatorVariantRow {
  private val fieldCount: Int = 18 //scalastyle:ignore magic.number
  
  val defaultHeaders: Seq[String] = {
    Seq(
      AggregatorColumnNames.marker,
      AggregatorColumnNames.pvalue,
      
      AggregatorColumnNames.zscore,
      AggregatorColumnNames.stderr,
      AggregatorColumnNames.beta,
      AggregatorColumnNames.odds_ratio,
      AggregatorColumnNames.eaf,
      AggregatorColumnNames.maf,
      AggregatorColumnNames.n,
      
      AggregatorColumnNames.mafCasesControls,
      AggregatorColumnNames.alleleCountCasesControls,
      AggregatorColumnNames.alleleCountCases,
      AggregatorColumnNames.alleleCountControls,
      AggregatorColumnNames.heterozygousCountCases,
      AggregatorColumnNames.heterozygousCountControls, 
      AggregatorColumnNames.homozygousCountCases,
      AggregatorColumnNames.homozygousCountControls).map(_.name) 
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
