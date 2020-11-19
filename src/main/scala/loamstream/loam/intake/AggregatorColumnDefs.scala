package loamstream.loam.intake

import loamstream.loam.intake.ColumnExpr.ExprOps
import scala.reflect.api.materializeTypeTag

/**
 * @author clint
 * Apr 17, 2020
 */
object AggregatorColumnDefs {
 
  import IntakeSyntax._
  import ColumnExpr.asDouble
  
  def marker(
      chromColumn: ColumnExpr[_],
      posColumn: ColumnExpr[_],
      refColumn: ColumnExpr[_],
      altColumn: ColumnExpr[_],
      destColumn: ColumnName = AggregatorColumnNames.marker,
      forceAlphabeticChromNames: Boolean = true,
      uppercaseAlleles: Boolean = true): MarkerColumnDef = {
    
    val chromExpr = {
      val chromStringColumn = chromColumn.asString
      
      if(forceAlphabeticChromNames) { ColumnTransforms.ensureAlphabeticChromNames(chromStringColumn) }
      else { chromStringColumn }
    }
    
    //"{chrom}_{pos}_{ref}_{alt}"
    val asString = strexpr"${chromExpr}_${posColumn}_${refColumn}_${altColumn}"
      
    val variantExpr: ColumnExpr[Variant] = {
      val variantExpr = asString.map(Variant.from)
      
      if(uppercaseAlleles) variantExpr.map(_.toUpperCase) else variantExpr
    }
    
    MarkerColumnDef(destColumn, variantExpr)
  }
  
  def just(columnName: ColumnName): NamedColumnDef[String] = NamedColumnDef(columnName)
  
  def pvalue(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.pvalue): NamedColumnDef[Double] = {
    
    simpleDoubleColumn(sourceColumn, destColumn) 
  }
  
  def stderr(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.stderr): NamedColumnDef[Double] = {
    
    simpleDoubleColumn(sourceColumn, destColumn)
  }
  
  def zscoreFrom(
      betaColumn: ColumnExpr[_], 
      stderrColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.zscore): NamedColumnDef[Double] = {
    
    val expr = asDouble(betaColumn) / asDouble(stderrColumn)
    
    negateIfFlipped(expr, destColumn)
  }
  
  def zscore(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.zscore): NamedColumnDef[Double] = {
    
    negateIfFlipped(sourceColumn, destColumn)
  }
  
  def beta(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.beta): NamedColumnDef[Double] = {
    
    negateIfFlipped(sourceColumn, destColumn)
  }
  
  def eaf(sourceColumn: ColumnExpr[_], destColumn: ColumnName = AggregatorColumnNames.eaf): NamedColumnDef[Double] = {
    val expr = asDouble(sourceColumn)
    
    NamedColumnDef(destColumn, expr, 1.0 - expr)
  }
  
  def oddsRatio(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.odds_ratio): NamedColumnDef[Double] = {
    
    val expr = asDouble(sourceColumn)

    NamedColumnDef(destColumn, expr, 1.0 / expr)
  }
  
  object PassThru {
    def marker(
        sourceColumn: ColumnExpr[String], 
        destColumn: ColumnName = AggregatorColumnNames.marker): NamedColumnDef[String] = {
      
      NamedColumnDef(destColumn, sourceColumn)
    }
    
    def pvalue(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.pvalue): NamedColumnDef[Double] = {
      
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def zscore(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.zscore): NamedColumnDef[Double] = {
      
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def stderr(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.stderr): NamedColumnDef[Double] = {
      
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def beta(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.beta): NamedColumnDef[Double] = {
      
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def oddsRatio(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.odds_ratio): NamedColumnDef[Double] = {
      
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def eaf(sourceColumn: ColumnExpr[_], destColumn: ColumnName = AggregatorColumnNames.eaf): NamedColumnDef[Double] = {
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def maf(sourceColumn: ColumnExpr[_], destColumn: ColumnName = AggregatorColumnNames.maf): NamedColumnDef[Double] = {
      NamedColumnDef(destColumn, asDouble(sourceColumn))
    }
    
    def n(sourceColumn: ColumnExpr[_], destColumn: ColumnName = AggregatorColumnNames.n): NamedColumnDef[Double] = {
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
