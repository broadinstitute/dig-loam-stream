package loamstream.loam.intake.aggregator

import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.UnsourcedColumnDef

/**
 * @author clint
 * Apr 17, 2020
 */
object ColumnDefs {
 
  import IntakeSyntax._
  
  def marker(
      chromColumn: ColumnExpr[_],
      posColumn: ColumnExpr[_],
      refColumn: ColumnExpr[_],
      altColumn: ColumnExpr[_],
      destColumn: ColumnName = ColumnNames.marker): UnsourcedColumnDef = {
    
    ColumnDef(
      destColumn,
      //"{chrom}_{pos}_{ref}_{alt}"
      strexpr"${chromColumn}_${posColumn}_${refColumn}_${altColumn}",
      //"{chrom}_{pos}_{alt}_{ref}"
      strexpr"${chromColumn}_${posColumn}_${altColumn}_${refColumn}")
  }
  
  def just(columnName: ColumnName): UnsourcedColumnDef = ColumnDef(columnName)
  
  def pvalue(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.pvalue): UnsourcedColumnDef = {
    simpleDoubleColumn(sourceColumn, destColumn) 
  }
  
  def stderr(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.stderr): UnsourcedColumnDef = {
    simpleDoubleColumn(sourceColumn, destColumn)
  }
  
  def zscoreFrom(
      betaColumn: ColumnExpr[_], 
      stderrColumn: ColumnExpr[_], 
      destColumn: ColumnName = ColumnNames.zscore): UnsourcedColumnDef = {
    
    val expr = asDouble(betaColumn) / asDouble(stderrColumn)
    
    negateIfFlipped(expr, destColumn)
  }
  
  def zscore(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = ColumnNames.zscore): UnsourcedColumnDef = {
    
    negateIfFlipped(sourceColumn, destColumn)
  }
  
  def beta(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.beta): UnsourcedColumnDef = {
    negateIfFlipped(sourceColumn, destColumn)
  }
  
  def eaf(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.eaf): UnsourcedColumnDef = {
    val expr = asDouble(sourceColumn)
    
    ColumnDef(destColumn, expr, 1.0 - expr)
  }
  
  def oddsRatio(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.odds_ratio): UnsourcedColumnDef = {
    val expr = asDouble(sourceColumn)

    ColumnDef(destColumn, expr, 1.0 / expr)
  }
  
  object PassThru {
    def marker(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.marker): UnsourcedColumnDef = {
      ColumnDef(destColumn, sourceColumn)
    }
    
    def pvalue(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.pvalue): UnsourcedColumnDef = {
      ColumnDef(destColumn, sourceColumn)
    }
    
    def zscore(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.zscore): UnsourcedColumnDef = {
      ColumnDef(destColumn, sourceColumn)
    }
    
    def stderr(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.stderr): UnsourcedColumnDef = {
      ColumnDef(destColumn, sourceColumn)
    }
    
    def beta(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.beta): UnsourcedColumnDef = {
      ColumnDef(destColumn, sourceColumn)
    }
    
    def oddsRatio(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.odds_ratio): UnsourcedColumnDef = {
      ColumnDef(destColumn, sourceColumn)
    }
    
    def eaf(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.eaf): UnsourcedColumnDef = {
      ColumnDef(destColumn, sourceColumn)
    }
    
    def maf(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.maf): UnsourcedColumnDef = {
      ColumnDef(destColumn, sourceColumn)
    }
    
    def n(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.n): UnsourcedColumnDef = {
      ColumnDef(destColumn, sourceColumn)
    }
  }

  //TODO: Something better, this makes potentially-superfluous .map() invocations
  private def asDouble(column: ColumnExpr[_]): ColumnExpr[Double] = column.asString.asDouble
      
  private def simpleDoubleColumn(
      sourceColumn: ColumnExpr[_],
      aggregatorName: ColumnName): UnsourcedColumnDef = ColumnDef(aggregatorName, asDouble(sourceColumn))
      
  def negateIfFlipped(sourceColumn: ColumnExpr[_], aggregatorName: ColumnName): UnsourcedColumnDef = {
    val expr = asDouble(sourceColumn)
    
    ColumnDef(aggregatorName, expr, expr.negate)
  }
}
