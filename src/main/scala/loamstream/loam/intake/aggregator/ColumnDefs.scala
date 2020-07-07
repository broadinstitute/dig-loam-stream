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
  
  def zscore(
      betaColumn: ColumnExpr[_], 
      stderrColumn: ColumnExpr[_], 
      destColumn: ColumnName = ColumnNames.zscore): UnsourcedColumnDef = {
    
    val expr = asDouble(betaColumn) / asDouble(stderrColumn)
    
    ColumnDef(destColumn, expr, expr.negate)
  }
  
  def beta(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.beta): UnsourcedColumnDef = {
    
    val expr = asDouble(sourceColumn)
    
    ColumnDef(destColumn, expr, expr.negate)
  }
  
  def eaf(sourceColumn: ColumnExpr[_], destColumn: ColumnName = ColumnNames.eaf): UnsourcedColumnDef = {
    val expr = asDouble(sourceColumn)
    
    ColumnDef(destColumn, expr, 1.0 - expr)
  }
  
  //TODO: Something better, this makes potentially-superfluous .map() invocations
  private def asDouble(column: ColumnExpr[_]): ColumnExpr[Double] = column.asString.asDouble
      
  private def simpleDoubleColumn(
      sourceColumn: ColumnExpr[_],
      aggregatorName: ColumnName): UnsourcedColumnDef = ColumnDef(aggregatorName, asDouble(sourceColumn))
}
