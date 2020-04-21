package loamstream.loam.intake.aggregator

import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.ColumnExpr
import loamstream.loam.intake.UnsourcedColumnDef
import loamstream.loam.intake.ColumnName

/**
 * @author clint
 * Apr 17, 2020
 */
object ColumnDefs {
  /*
   * def rowDef(source: CsvSource): RowDef = {
    import ColumnNames._
    
    val a1Freq = A1FREQ.asDouble
    val beta = BETA.asDouble
    val zscore = BETA.asDouble / SE.asDouble
    
    val otherColumns = Seq(
      ColumnDef(aggregator.ColumnNames.pvalue, P_BOLT_LMM),
      ColumnDef(aggregator.ColumnNames.stderr, SE),
      ColumnDef(aggregator.ColumnNames.beta, beta, beta.negate),
      ColumnDef(aggregator.ColumnNames.eaf, a1Freq, a1Freq.complement),
      ColumnDef(aggregator.ColumnNames.zscore, zscore, zscore.negate))
  }
   */
  
  import IntakeSyntax._
  
  def marker(
      chromColumn: ColumnExpr[_],
      posColumn: ColumnExpr[_],
      refColumn: ColumnExpr[_],
      altColumn: ColumnExpr[_]): UnsourcedColumnDef = {
    
    ColumnDef(
      ColumnNames.marker,
      //"{chrom}_{pos}_{ref}_{alt}"
      strexpr"${chromColumn}_${posColumn}_${refColumn}_${altColumn}",
      //"{chrom}_{pos}_{alt}_{ref}"
      strexpr"${chromColumn}_${posColumn}_${altColumn}_${refColumn}")
  }
  
  def pvalue(sourceColumn: ColumnExpr[_]): UnsourcedColumnDef = simpleDoubleColumn(ColumnNames.pvalue, sourceColumn) 
  
  def stderr(sourceColumn: ColumnExpr[_]): UnsourcedColumnDef = simpleDoubleColumn(ColumnNames.stderr, sourceColumn)
  
  def zscore(betaColumn: ColumnExpr[_], stderrColumn: ColumnExpr[_]): UnsourcedColumnDef = {
    
    val expr = asDouble(betaColumn) / asDouble(stderrColumn)
    
    ColumnDef(ColumnNames.zscore, expr, expr.negate)
  }
  
  def beta(sourceColumn: ColumnExpr[_]): UnsourcedColumnDef = {
    val expr = asDouble(sourceColumn)
    
    ColumnDef(ColumnNames.beta, expr, expr.negate)
  }
  
  def eaf(sourceColumn: ColumnExpr[_]): UnsourcedColumnDef = {
    val expr = asDouble(sourceColumn)
    
    ColumnDef(ColumnNames.eaf, expr, expr.complement),
  }
  
  //TODO: Something better, this makes potentially-superfluous .map() invocations
  private def asDouble(column: ColumnExpr[_]): ColumnExpr[Double] = column.asString.asDouble
      
  private def simpleDoubleColumn(
      aggregatorName: ColumnName, 
      sourceColumn: ColumnExpr[_]): UnsourcedColumnDef = ColumnDef(aggregatorName, asDouble(sourceColumn))

  /*
   * TODO
  val odds_ratio: ColumnName = "odds_ratio".asColumnName
  val maf: ColumnName =        "maf".asColumnName
   */
      
  object Example {
    object ColumnNames {
      val CHR = "CHR".asColumnName
      val BP = "BP".asColumnName
      val ALLELE1 = "ALLELE1".asColumnName
      val ALLELE0 = "ALLELE0".asColumnName
      val A1FREQ = "A1FREQ".asColumnName 
      val INFO = "INFO".asColumnName
      val BETA = "BETA".asColumnName
      val SE = "SE".asColumnName
      val P_BOLT_LMM = "P_BOLT_LMM".asColumnName
      
      val VARID = "VARID".asColumnName
    }
    
    def rowDef(source: CsvSource): RowDef = {
      import ColumnNames._
    
      val varId = marker(chromColumn = CHR, posColumn = BP, refColumn = ALLELE0, altColumn = ALLELE1)
        
      val otherColumns = Seq(
        pvalue(P_BOLT_LMM),
        stderr(SE),
        beta(BETA),
        eaf(A1FREQ),
        zscore(BETA, SE))
      
      UnsourcedRowDef(varId, otherColumns).from(source)
    }
  }
}
