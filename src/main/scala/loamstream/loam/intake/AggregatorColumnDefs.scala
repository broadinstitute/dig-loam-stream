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
      
      ColumnTransforms.normalizeChromNames {
        if(forceAlphabeticChromNames) { ColumnTransforms.ensureAlphabeticChromNames(chromStringColumn) }
        else { chromStringColumn }
      }
    }
    
    val variantExpr: ColumnExpr[Variant] = {
      val asVariant = for {
        chrom <- chromExpr
        pos <- posColumn.asString.trim
        ref <- refColumn.asString.trim
        alt <- altColumn.asString.trim
      } yield Variant.from(chrom, pos, ref, alt)
      
      if(uppercaseAlleles) asVariant.map(_.toUpperCase) else asVariant
    }
    
    MarkerColumnDef(destColumn, variantExpr)
  }
  
  def just(columnName: ColumnName): HandlesFlipsColumnDef[String] = AnonColumnDef(columnName)
  
  def pvalue(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.pvalue): HandlesFlipsColumnDef[Double] = {
    
    simpleDoubleColumn(sourceColumn, destColumn) 
  }
  
  def stderr(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.stderr): HandlesFlipsColumnDef[Double] = {
    
    simpleDoubleColumn(sourceColumn, destColumn)
  }
  
  def zscoreFrom(
      betaColumn: ColumnExpr[_], 
      stderrColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.zscore): HandlesFlipsColumnDef[Double] = {
    
    val expr = asDouble(betaColumn) / asDouble(stderrColumn)
    
    negateIfFlipped(expr, destColumn)
  }
  
  def zscore(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.zscore): HandlesFlipsColumnDef[Double] = {
    
    negateIfFlipped(sourceColumn, destColumn)
  }
  
  def beta(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.beta): HandlesFlipsColumnDef[Double] = {
    
    negateIfFlipped(sourceColumn, destColumn)
  }
  
  def eaf(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.eaf): HandlesFlipsColumnDef[Double] = {
    
    val expr = asDouble(sourceColumn)
    
    AnonColumnDef(expr, 1.0 - expr)
  }
  
  def oddsRatio(
      sourceColumn: ColumnExpr[_], 
      destColumn: ColumnName = AggregatorColumnNames.odds_ratio): HandlesFlipsColumnDef[Double] = {
    
    val expr = asDouble(sourceColumn)

    AnonColumnDef(expr, 1.0 / expr)
  }
  
  object PassThru {
    def marker(
        sourceColumn: ColumnExpr[String], 
        destColumn: ColumnName = AggregatorColumnNames.marker): HandlesFlipsColumnDef[String] = {
      
      AnonColumnDef(sourceColumn)
    }
    
    def pvalue(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.pvalue): HandlesFlipsColumnDef[Double] = {
      
      AnonColumnDef(asDouble(sourceColumn))
    }
    
    def zscore(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.zscore): HandlesFlipsColumnDef[Double] = {
      
      AnonColumnDef(asDouble(sourceColumn))
    }
    
    def stderr(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.stderr): HandlesFlipsColumnDef[Double] = {
      
      AnonColumnDef(asDouble(sourceColumn))
    }
    
    def beta(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.beta): HandlesFlipsColumnDef[Double] = {
      
      AnonColumnDef(asDouble(sourceColumn))
    }
    
    def oddsRatio(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.odds_ratio): HandlesFlipsColumnDef[Double] = {
      
      AnonColumnDef(asDouble(sourceColumn))
    }
    
    def eaf(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.eaf): HandlesFlipsColumnDef[Double] = {
      
      AnonColumnDef(asDouble(sourceColumn))
    }
    
    def maf(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.maf): HandlesFlipsColumnDef[Double] = {
      
      AnonColumnDef(asDouble(sourceColumn))
    }
    
    def n(
        sourceColumn: ColumnExpr[_], 
        destColumn: ColumnName = AggregatorColumnNames.n): HandlesFlipsColumnDef[Double] = {
      AnonColumnDef(asDouble(sourceColumn))
    }
  }

  private def simpleDoubleColumn(
      sourceColumn: ColumnExpr[_],
      aggregatorName: ColumnName): HandlesFlipsColumnDef[Double] = {
    
    AnonColumnDef(asDouble(sourceColumn))
  }
      
  def negateIfFlipped(sourceColumn: ColumnExpr[_], aggregatorName: ColumnName): HandlesFlipsColumnDef[Double] = {
    val expr = asDouble(sourceColumn)
    
    AnonColumnDef(expr, expr.negate)
  }
}
