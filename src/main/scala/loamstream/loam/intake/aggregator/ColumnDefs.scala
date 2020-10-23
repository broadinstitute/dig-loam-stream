package loamstream.loam.intake.aggregator

import loamstream.loam.intake.IntakeSyntax

/**
 * @author clint
 * Apr 17, 2020
 */
object ColumnDefs {
 
  import IntakeSyntax._
  import ColumnExpr.asDouble
  
  def marker(
      chromColumn: ColumnExpr[_],
      posColumn: ColumnExpr[_],
      refColumn: ColumnExpr[_],
      altColumn: ColumnExpr[_],
      destColumn: ColumnName = ColumnNames.marker): NamedColumnDef[String] = {
    
    NamedColumnDef(
      destColumn,
      //"{chrom}_{pos}_{ref}_{alt}"
      strexpr"${chromColumn}_${posColumn}_${refColumn}_${altColumn}",
      //"{chrom}_{pos}_{alt}_{ref}"
      strexpr"${chromColumn}_${posColumn}_${altColumn}_${refColumn}")
  }
  
  def just(columnName: ColumnName): NamedColumnDef[String] = NamedColumnDef(columnName)
  
  def pvalue(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = ColumnNames.pvalue): NamedColumnDef[Double] = {
    
    simpleDoubleColumn(sourceColumn, destColumn) 
  }
  
  def stderr(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = ColumnNames.stderr): NamedColumnDef[Double] = {
    
    simpleDoubleColumn(sourceColumn, destColumn)
  }
  
  def zscoreFrom(
      betaColumn: ColumnExpr[_], 
      stderrColumn: ColumnExpr[_], 
      destColumn: ColumnName = ColumnNames.zscore): NamedColumnDef[Double] = {
    
    val expr = asDouble(betaColumn) / asDouble(stderrColumn)
    
    negateIfFlipped(expr, destColumn)
  }
  
  def zscore(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = ColumnNames.zscore): NamedColumnDef[Double] = {
    
    negateIfFlipped(sourceColumn, destColumn)
  }
  
  def beta(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = ColumnNames.beta): NamedColumnDef[Double] = {
    
    negateIfFlipped(sourceColumn, destColumn)
  }
  
  def eaf(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.eaf): NamedColumnDef[Double] = {
    val expr = asDouble(sourceColumn)
    
    NamedColumnDef(destColumn, expr, 1.0 - expr)
  }
  
  def oddsRatio(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = ColumnNames.odds_ratio): NamedColumnDef[Double] = {
    
    val expr = asDouble(sourceColumn)

    NamedColumnDef(destColumn, expr, 1.0 / expr)
  }
  
  object PassThru {
    def marker(
        sourceColumn: ColumnExpr[String], 
        destColumn: ColumnName = ColumnNames.marker): NamedColumnDef[String] = {
      
      NamedColumnDef(destColumn, sourceColumn)
    }
    
    def pvalue(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = ColumnNames.pvalue): NamedColumnDef[Double] = {
      
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def zscore(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = ColumnNames.zscore): NamedColumnDef[Double] = {
      
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def stderr(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = ColumnNames.stderr): NamedColumnDef[Double] = {
      
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def beta(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = ColumnNames.beta): NamedColumnDef[Double] = {
      
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def oddsRatio(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = ColumnNames.odds_ratio): NamedColumnDef[Double] = {
      
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def eaf(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.eaf): NamedColumnDef[Double] = {
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def maf(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.maf): NamedColumnDef[Double] = {
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def n(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.n): NamedColumnDef[Double] = {
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
  }

  private def simpleDoubleColumn(
      sourceColumn: ColumnExpr[_],
      aggregatorName: ColumnName): NamedColumnDef[Double] = {
    
    NamedColumnDef(aggregatorName, asDouble(sourceColumn))
  }
      
  def negateIfFlipped(sourceColumn: ColumnExpr[_], aggregatorName: ColumnName): NamedColumnDef[Double] = {
    val expr = asDouble(sourceColumn)
    
    NamedColumnDef(aggregatorName, expr, expr.negate)
  }
}
