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
      diseaseTermId = columns.diseaseTermId(row),
      diseaseTermName = columns.diseaseTermName(row),
      annotation = columns.annotation(row).name,    // annotation type, e.g. binding_site
      method = columns.method(row),  // e.g. MAC2
      source = columns.source(row),   // e.g. ATAC-seq-peak
      assay = columns.assay(row),   // e.g. ATAC-seq
      collection = columns.collection(row), 
      chromosome = columns.chromosome(row),
      start = columns.start(row),
      end = columns.end(row),
      state = columns.state(row), 
      targetGene = columns.targetGene(row),    //TODO  only for annotation_type == "target_gene_prediction"
      targetGeneStart = columns.targetGeneStart(row),    // TODO only for annotation_type == "target_gene_prediction"
      targetGeneEnd = columns.targetGeneEnd(row), //TODO  only for annotation_type == "target_gene_prediction"
      variant = columns.variant(row), //TODO  only for annotation_type == "variant_to_gene"
      gene = columns.gene(row), //TODO  only for annotation_type == "variant_to_gene"
      score = columns.score(row), //TODO  only for annotation_type == "variant_to_gene"
      info = columns.info(row), //TODO  only for annotation_type == "variant_to_gene"
      ensemblId = columns.ensemblId(row),    // only for annotation_type == "gene_expression"
      nSamples = columns.nSamples(row),    // only for annotation_type == "gene_expression"
      tpmForAllSamples = columns.tpmForAllSamples(row),    // only for annotation_type == "gene_expression"
      minTpm = columns.minTpm(row),    // only for annotation_type == "gene_expression"
      firstQuTpm = columns.firstQuTpm(row),    // only for annotation_type == "gene_expression"
      medianTpm = columns.medianTpm(row),    // only for annotation_type == "gene_expression"
      meanTpm = columns.meanTpm(row),    // only for annotation_type == "gene_expression"
      thirdQuTpm = columns.thirdQuTpm(row),    // only for annotation_type == "gene_expression"
      maxTpm = columns.maxTpm(row)    // only for annotation_type == "gene_expression"
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
    val diseaseTermId = LiteralColumnExpr(ann.diseaseTermId)
    val diseaseTermName = LiteralColumnExpr(ann.diseaseTermName)
    val annotation = LiteralColumnExpr(ann.annotationType)
    val method = LiteralColumnExpr(ann.method)
    val source = LiteralColumnExpr(ann.source)
    val assay = LiteralColumnExpr(ann.assay)
    val collection = LiteralColumnExpr(ann.collection)
    val chromosome: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.GeneExpression => LiteralColumnExpr(None)
        case _ =>
          ColumnTransforms.ensureAlphabeticChromNamesOpt {
            ColumnTransforms.normalizeChromNamesOpt {
              ColumnName("chromosome").or(ColumnName("chr")).or(ColumnName("chrom")).asOptionWithNaValues
            }
          }
      }
    }

    val start = ann.annotationType match {
      case vtg @ AnnotationType.VariantToGene => ColumnName("location").asOptionWithNaValues.asLongOption
      case ge @ AnnotationType.GeneExpression => LiteralColumnExpr(None)
      case _ => ColumnName("start").or(ColumnName("chromStart")).asOptionWithNaValues.asLongOption
    }

    val end = ann.annotationType match {
      case vtg @ AnnotationType.VariantToGene => ColumnName("location").asOptionWithNaValues.asLongOption
      case ge @ AnnotationType.GeneExpression => LiteralColumnExpr(None)
      case _ => ColumnName("end").or(ColumnName("chromEnd")).asOptionWithNaValues.asLongOption
    }

    private val stateOrNameOpt: ColumnExpr[Option[String]] = {
      ColumnName("state").or(ColumnName("name")).asOptionWithNaValues
    }
    
    // the annotation name needs to be harmonized (accessible chromatin is special!)
    val state: ColumnExpr[Option[String]] = {
      val baseExpr = (ann.annotationType match {
        case x @ (AnnotationType.AccessibleChromatin | AnnotationType.VariantToGene | AnnotationType.GeneExpression) =>
          LiteralColumnExpr(Option(x.name))
        case _ => stateOrNameOpt 
      }).map {
        //Strip any leading digit prefix, if present
        _.map(_.replaceAll("^\\d+_", ""))
      }

      //Look up the base value in harmonizedStates mapping, and use the mapped value, if one is found.
      ann.harmonizedStates match {
        case Some(hs) => baseExpr.map(_.flatMap(hs.get))
        case None => baseExpr
      }
    }.spacesToUnderscores

    //private def ifTargetGenePrediction(regex: Regex): ColumnExpr[Option[String]] = {
    //  ann.annotationType match {
    //    case AnnotationType.TargetGenePredictions =>
    //      stateOrNameOpt.map {
    //        _.collect { case regex(v) => v }
    //      }
    //    case _ => LiteralColumnExpr(None)
    //  }
    //}
    
    //val targetGene: ColumnExpr[Option[String]] = ifTargetGenePrediction(Regexes.targetGeneName)
    //val targetGeneStart: ColumnExpr[Option[Long]] = ifTargetGenePrediction(Regexes.targetGeneStart).map(_.map(_.toLong))
    //val targetGeneEnd: ColumnExpr[Option[Long]] = ifTargetGenePrediction(Regexes.targetGeneEnd).map(_.map(_.toLong))
    val targetGene: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.TargetGenePredictions => ColumnName("targetGene").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val targetGeneStart: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.TargetGenePredictions => ColumnName("TargetGeneElementStart").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val targetGeneEnd: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.TargetGenePredictions => ColumnName("TargetGeneElementEnd").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }

    val variant: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.VariantToGene => ColumnName("variant").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val gene: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case (AnnotationType.VariantToGene | AnnotationType.GeneExpression) => ColumnName("gene").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val score: ColumnExpr[Option[Long]] = {
      ann.annotationType match {
        case AnnotationType.VariantToGene => ColumnName("score").asOptionWithNaValues.asLongOption.map(_.map(_.toLong))
        case _ => LiteralColumnExpr(None)
      }
    }
    val info: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.VariantToGene => ColumnName("info").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val ensemblId: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.GeneExpression => ColumnName("ensemblID").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val nSamples: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.GeneExpression => ColumnName("#samples").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val tpmForAllSamples: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.GeneExpression => ColumnName("TPM_for_all_samples").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val minTpm: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.GeneExpression => ColumnName("min_TPM").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val firstQuTpm: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.GeneExpression => ColumnName("1stQu_TPM").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val medianTpm: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.GeneExpression => ColumnName("median_TPM").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val meanTpm: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.GeneExpression => ColumnName("mean_TPM").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val thirdQuTpm: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.GeneExpression => ColumnName("3rdQu_TPM").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }
    val maxTpm: ColumnExpr[Option[String]] = {
      ann.annotationType match {
        case AnnotationType.GeneExpression => ColumnName("max_TPM").asOptionWithNaValues.map(_.map(_.toString))
        case _ => LiteralColumnExpr(None)
      }
    }

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
  
  private object Regexes {
    val targetGeneStart = """^.*\:(\d+)-.*$""".r
    val targetGeneEnd = """^.*\:\d+-(\d+).*$""".r
    val targetGeneName = """^.*\:\d+-\d+_(.*)$""".r
  }
}
