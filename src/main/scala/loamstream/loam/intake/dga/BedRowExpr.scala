package loamstream.loam.intake.dga

import loamstream.loam.intake.DataRowParser
import loamstream.loam.intake.DataRow
import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.ColumnName
import loamstream.loam.intake.ColumnExpr
import com.sun.org.apache.xalan.internal.xsltc.compiler.LiteralExpr
import loamstream.loam.intake.LiteralColumnExpr
import loamstream.loam.intake.ColumnTransforms

/**
 * @author clint
 * Jan 20, 2021
 */
final case class BedRowExpr(annotation: Annotation) extends DataRowParser[BedRow] {
  private val columns = new BedRowExpr.Columns(this)
  
  override def apply(row: DataRow): BedRow = {
    BedRow(
      dataset = columns.dataset(row),
      biosampleId = columns.biosampleId(row),    // e.g. UBERON:293289
      biosampleType = columns.biosampleType(row),
      biosample = columns.biosample(row),
      tissueId = columns.tissueId(row),   // e.g. UBERON:293289
      tissue = columns.tissue(row),
      annotation = columns.annotation(row),    // annotation type, e.g. binding_site
      category = columns.category(row),
      method = columns.method(row),  // e.g. MAC2
      source = columns.source(row),   // e.g. ATAC-seq-peak
      assay = columns.assay(row),   // e.g. ATAC-seq
      collection = columns.collection(row), // e.g. ENCODE
      chromosome = columns.chromosome(row),
      start = columns.start(row),
      end = columns.end(row),
      state = columns.state(row), //was name
      targetGene = columns.targetGene(row),    //TODO  only for annotation_type == "target_gene_prediction"
      targetGeneStart = columns.targetGeneStart(row),    // TODO only for annotation_type == "target_gene_prediction"
      targetGeneEnd = columns.targetGeneEnd(row) //TODO  only for annotation_type == "target_gene_prediction"
      )

     /* for row in df.itertuples(index=False, name='Annotation'):

          	def col(name, alt=None):
                return getattr(row, name, getattr(row, alt, None) if alt else None)

            # the annotation name needs to be harmonized (accessible chromatin is special!)
            annotation_name = typ if typ == 'accessible_chromatin' else col('state', 'name')

            # some annotation names need to be harmonized
            if annot.harmonized_states is not None:
                annotation_name = annot.harmonized_states.get(re.sub(r'^\d+_', '', annotation_name))

            # verify that the annotation name exists
            if annotation_name is None or annotation_name == '.':
                logging.warning('Unknown annotation name: %s (%s); skipping region...', annotation_name, annot.annotation_id)
                continue

            # parse the chromosome
            try:
                chromosome = parse_chromosome(col('chr', 'chrom'))
            except ValueError as e:
                logging.warning('%s', e)
                continue

            entry = {
                'chromosome': chromosome,
                'start': col('start', 'chromStart'),
                'end': col('end', 'chromEnd'),
                'biosample': annot.biosample_id,
                'method': annot.annotation_method,
                'name': cap_case_str(annotation_name),
                'score': col('value', 'score'),
                'strand': col('strand'),
                'thickStart': col('thickStart'),
                'thickEnd': col('thickEnd'),
                'itemRgb': col('itemRgb'),
                'blockCount': col('blockCount'),
                'blockSizes': col('blockSizes'),
                'blockStarts': col('blockStarts'),
            }

            # transform the record and write it
            dataset.write(entry)
     */
  }
}

object BedRowExpr {
  object Implicits {
    implicit final class ColumnExprOps[A](val expr: ColumnExpr[A]) extends AnyVal {
      def asDoubleWithNaValues(
          values: Set[String] = Set.empty, 
          usePandasDefaults: Boolean = true): ColumnExpr[Double] = {
        
        val naChars = {
          val pandasDefaults = if(usePandasDefaults) pandasDefaultNaValues else Set.empty
          
          asJavaSet(pandasDefaults, values)
        }
        
        val stringExpr = expr.asString.trim
        
        stringExpr.flatMap { s =>
          if(naChars.contains(s)) { LiteralColumnExpr(Double.NaN) }
          else { stringExpr.asDouble }
        }
      }
    }
  }
  
  final case class Columns(expr: BedRowExpr) {
    import BedRowExpr.Implicits.ColumnExprOps
    
    val dataset = LiteralColumnExpr(expr.annotation.annotationId)
    val biosampleId = LiteralColumnExpr(expr.annotation.biosampleId)
    val biosampleType = LiteralColumnExpr(expr.annotation.biosampleType)
    val biosample = LiteralColumnExpr(expr.annotation.biosample)
    val tissueId = LiteralColumnExpr(expr.annotation.tissueId)
    val tissue = LiteralColumnExpr(expr.annotation.tissue)
    val annotation = LiteralColumnExpr(expr.annotation.annotationType).map(_.replaceAll("\\s+", "_"))
    val category = LiteralColumnExpr(expr.annotation.category)
    val method = LiteralColumnExpr(expr.annotation.method)
    val source = LiteralColumnExpr(expr.annotation.source)
    val assay = LiteralColumnExpr(expr.annotation.assay)
    val collection = LiteralColumnExpr(expr.annotation.collection)
    val chromosome = {
      ColumnTransforms.ensureAlphabeticChromNames {
        ColumnTransforms.normalizeChromNames {
          ColumnName("chromosome").or(ColumnName("chr")).or(ColumnName("chrom"))
        }
      }.trim
    }
    val start = ColumnName("start").or(ColumnName("chromStart")).asLong
    val end = ColumnName("end").or(ColumnName("chromEnd")).asLong
    // the annotation name needs to be harmonized (accessible chromatin is special!)
    val state = {
      expr.annotation.annotationType match {
        case ac @ "accessible_chromatin" => LiteralColumnExpr(ac) 
        case _ => ColumnName("state").or(ColumnName("name")) 
      }
      
      /* TODO: is this still relevant?
       * if annot.harmonized_states is not None:
                annotation_name = annot.harmonized_states.get(re.sub(r'^\d+_', '', annotation_name))
       */
    }
    
    private def ifTargetGenePrediction(column: String): ColumnExpr[Option[String]] = {
      expr.annotation.annotationType match {
        case "target_gene_prediction" => ColumnName(column).asOption
        case _ => LiteralColumnExpr(None) 
      }
    }
    
    val targetGene: ColumnExpr[Option[String]] = ifTargetGenePrediction("target_gene")
    val targetGeneStart: ColumnExpr[Option[Long]] = ifTargetGenePrediction("target_gene_start").map(_.map(_.toLong))
    val targetGeneEnd: ColumnExpr[Option[Long]] = ifTargetGenePrediction("target_gene_end").map(_.map(_.toLong))
  }
  
  //TODO: Handle '.' values?
  
  //See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html 
  private val pandasDefaultNaValues: Set[String] = Set(
       "", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", 
       "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "n/a", "nan", "null")
  
  private def asJavaSet[A](as: Iterable[A]*): java.util.Set[A] = {
    val result = new java.util.HashSet[A]

    as.foreach(_.foreach(result.add))
        
    result
  }
}
