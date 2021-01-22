package loamstream.loam.intake.dga

import loamstream.loam.intake.DataRowParser
import loamstream.loam.intake.DataRow
import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.ColumnName
import loamstream.loam.intake.ColumnExpr
import com.sun.org.apache.xalan.internal.xsltc.compiler.LiteralExpr
import loamstream.loam.intake.LiteralColumnExpr

/**
 * @author clint
 * Jan 20, 2021
 */
object BedRowExpr extends DataRowParser[BedRow] {
  override def apply(row: DataRow): BedRow = {
    BedRow(
      chr = Columns.chr(row),
      start = Columns.start(row),
      end = Columns.end(row),
      state = Columns.state(row),
      value = Columns.value(row),
      strand = Columns.strand(row),
      thickStart = Columns.thickStart(row),
      thickEnd = Columns.thickEnd(row),
      itemRgb = Columns.itemRgb(row),
      blockCount = Columns.blockCount(row),
      blockSizes = Columns.blockSizes(row),
      blockStarts = Columns.blockStarts(row),
      chrom = Columns.chrom.applyOpt(row),
      chromStart = Columns.chromStart.applyOpt(row),
      chromEnd = Columns.chromEnd.applyOpt(row),
      name = Columns.name.applyOpt(row),
      score = Columns.score.applyOpt(row)) 
  }

  object Columns {
    val chr = ColumnName("chr").trim
    val start = ColumnName("start").asInt
    val end = ColumnName("end").asInt
    val state = ColumnName("state").trim
    val value = ColumnName("value").asDoubleWithNaValues(values = Set("."))
    val strand = ColumnName("strand").trim
    val thickStart = ColumnName("thickStart").asInt
    val thickEnd = ColumnName("thickEnd").asInt
    val itemRgb = ColumnName("itemRgb").trim
    val blockCount = ColumnName("blockCount").asInt
    val blockSizes = ColumnName("blockSizes").asInt
    val blockStarts = ColumnName("blockStarts").asInt
    val chrom = ColumnName("chrom").trim
    val chromStart = ColumnName("chromStart").asInt
    val chromEnd = ColumnName("chromEnd").asInt
    val name = ColumnName("name").trim
    val score = ColumnName("score").asDoubleWithNaValues(values = Set("."))
  }

  //See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html 
  private val pandasDefaultNaValues: Set[String] = Set(
       "", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", 
       "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "n/a", "nan", "null")
  
  private def asJavaSet[A](as: Iterable[A]*): java.util.Set[A] = {
    val result = new java.util.HashSet[A]

    as.foreach(_.foreach(result.add))
        
    result
  }
       
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
