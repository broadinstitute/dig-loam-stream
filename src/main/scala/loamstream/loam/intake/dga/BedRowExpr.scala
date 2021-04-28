package loamstream.loam.intake.dga

import loamstream.loam.intake.DataRowParser
import loamstream.loam.intake.DataRow
import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.ColumnName
import loamstream.loam.intake.ColumnExpr
import com.sun.org.apache.xalan.internal.xsltc.compiler.LiteralExpr
import loamstream.loam.intake.LiteralColumnExpr
import loamstream.loam.intake.ColumnTransforms
import scala.util.Try
import scala.util.matching.Regex
import scala.util.Success
import scala.util.Failure

/**
 * @author clint
 * Jan 20, 2021
 */
final case class BedRowExpr(annotation: Annotation, failFast: Boolean = false) extends DataRowParser[Try[BedRow]] {
  private val columns = new BedRowExpr.Columns(annotation)
  
  override def apply(row: DataRow): Try[BedRow] = {
    def requiredField[A](oa: Option[A], name: String): A = {
      require(oa.isDefined, s"Required field '${name}' failed to parse.")
      
      oa.get
    }
    
    val attempt = Try(BedRow(
      dataset = columns.dataset(row),
      biosampleId = columns.biosampleId(row),    // e.g. UBERON:293289
      biosampleType = columns.biosampleType(row),
      biosample = columns.biosample(row),
      tissueId = columns.tissueId(row),   // e.g. UBERON:293289
      tissue = columns.tissue(row),
      annotation = columns.annotation(row).name,    // annotation type, e.g. binding_site
      category = columns.category(row),
      method = columns.method(row),  // e.g. MAC2
      source = columns.source(row),   // e.g. ATAC-seq-peak
      assay = columns.assay(row),   // e.g. ATAC-seq
      collection = columns.collection(row), 
      chromosome = requiredField(columns.chromosome(row), "chromosome"),
      start = requiredField(columns.start(row), "start"),
      end = requiredField(columns.end(row), "end"),
      state = requiredField(columns.state(row), "state"), 
      targetGene = columns.targetGene(row),    //TODO  only for annotation_type == "target_gene_prediction"
      targetGeneStart = columns.targetGeneStart(row),    // TODO only for annotation_type == "target_gene_prediction"
      targetGeneEnd = columns.targetGeneEnd(row) //TODO  only for annotation_type == "target_gene_prediction"
      ))
      
    attempt match {
      case f @ Failure(e) => if(failFast) throw e else f    
      case a => a
    }
  }
}

object BedRowExpr {
  object Implicits {
    implicit final class ColumnExprOps[A](val expr: ColumnExpr[A]) {
      def asOptionWithNaValues(implicit ev: A =:= String): ColumnExpr[Option[String]] = expr.map { a =>
        val s = a.trim
        
        if(pandasDefaultNaValues.contains(s)) { None }
        else { Some(s) }
      }
      
      def spacesToUnderscores(implicit ev: A =:= String): ColumnExpr[String] = {
        ColumnTransforms.normalizeSpaces(expr.asString)
      }
    }
    
    implicit final class ColumnExprOptionOps[A](val expr: ColumnExpr[Option[A]]) {
      def asOptionWithNaValues(implicit ev: A =:= String): ColumnExpr[Option[String]] = expr.map { aOpt =>
        aOpt.map(_.trim).filterNot(pandasDefaultNaValues.contains)
      }
      
      def asLongOption(implicit ev: A =:= String): ColumnExpr[Option[Long]] = expr.map(_.map(a => ev(a).toLong))
      
      def remove(regex: String)(implicit ev: A =:= String): ColumnExpr[Option[String]] = {
        expr.map(_.map(_.replaceAll(regex, "")))
      }
      
      def spacesToUnderscores(implicit ev: A =:= String): ColumnExpr[Option[String]] = {
        ColumnTransforms.normalizeSpacesOpt(expr.map(_.map(ev)))
      }
    }
  }
  
  final case class Columns(ann: Annotation) {
    import BedRowExpr.Implicits.ColumnExprOps
    import BedRowExpr.Implicits.ColumnExprOptionOps
    
    val dataset = LiteralColumnExpr(ann.annotationId)
    val biosampleId = LiteralColumnExpr(ann.biosampleId)
    val biosampleType = LiteralColumnExpr(ann.biosampleType)
    val biosample = LiteralColumnExpr(ann.biosample)
    val tissueId = LiteralColumnExpr(ann.tissueId)
    val tissue = LiteralColumnExpr(ann.tissue).spacesToUnderscores
    val annotation = LiteralColumnExpr(ann.annotationType)
    val category = LiteralColumnExpr(ann.category).spacesToUnderscores
    val method = LiteralColumnExpr(ann.method)
    val source = LiteralColumnExpr(ann.source)
    val assay = LiteralColumnExpr(ann.assay)
    val collection = LiteralColumnExpr(ann.collection)
    val chromosome = {
      ColumnTransforms.ensureAlphabeticChromNamesOpt {
        ColumnTransforms.normalizeChromNamesOpt {
          ColumnName("chromosome").or(ColumnName("chr")).or(ColumnName("chrom")).asOptionWithNaValues
        }
      }
    }
    val start = {
      ColumnName("start").or(ColumnName("chromStart")).asOptionWithNaValues.asLongOption
    }
    val end = ColumnName("end").or(ColumnName("chromEnd")).asOptionWithNaValues.asLongOption
    
    // the annotation name needs to be harmonized (accessible chromatin is special!)
    val state: ColumnExpr[Option[String]] = {
      val baseExpr = (ann.annotationType match {
        case ac @ AnnotationType.AccessibleChromatin => LiteralColumnExpr(Option(ac.name)) 
        case _ => ColumnName("state").or(ColumnName("name")).asOptionWithNaValues 
      }).map {
        //Strip any leading digit prefix, if present
        _.map(_.replaceAll("^\\d+_", ""))
      }

      //Look up the base value in harmnoizedStates mapping, and use the mapped value, if one is found. 
      ann.harmonizedStates match {
        case Some(hs) => baseExpr.map(_.flatMap(hs.get))
        case None => baseExpr
      }
    }.spacesToUnderscores

    private def ifTargetGenePrediction(column: String): ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.TargetGenePredictions => ColumnName(column).asOptionWithNaValues
        case _ => LiteralColumnExpr(None) 
      }
    }
    
    val targetGene: ColumnExpr[Option[String]] = ifTargetGenePrediction("target_gene")
    val targetGeneStart: ColumnExpr[Option[Long]] = ifTargetGenePrediction("target_gene_start").map(_.map(_.toLong))
    val targetGeneEnd: ColumnExpr[Option[Long]] = ifTargetGenePrediction("target_gene_end").map(_.map(_.toLong))
    
    val strand: ColumnExpr[Option[Strand]] = ColumnName("strand").asOption.map(_.flatMap(Strand.fromString))
  }
  
  //See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html 
  private val pandasDefaultNaValues: java.util.Set[String] = asJavaSet(
       "", ".", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", 
       "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "n/a", "nan", "null")
  
  private def asJavaSet[A](as: A*): java.util.Set[A] = {
    val result = new java.util.HashSet[A]

    as.foreach(result.add)
        
    result
  }
}
