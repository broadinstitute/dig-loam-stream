package loamstream.loam.intake

import loamstream.util.Sequence


/**
 * @author clint
 * Dec 17, 2019
 */
final case class NamedColumnDef[A](
  name: ColumnName,
  expr: ColumnExpr[A],
  exprWhenFlipped: Option[ColumnExpr[A]]) extends ColumnDef[A]

object NamedColumnDef {
  def apply[A](
    name: ColumnName, 
    getValueFromSource: ColumnExpr[A],
    getValueFromSourceWhenFlipNeeded: ColumnExpr[A]): NamedColumnDef[A] = {
    
    new NamedColumnDef(name, getValueFromSource, Some(getValueFromSourceWhenFlipNeeded))
  }
  
  def apply[A](
    name: String, 
    srcColumn: ColumnExpr[A],
    srcColumnWhenFlipNeeded: ColumnExpr[A]): NamedColumnDef[A] = {
    
    apply(ColumnName(name), srcColumn, srcColumnWhenFlipNeeded)
  }
  
  def apply[A](name: String, srcColumn: ColumnExpr[A]): NamedColumnDef[A] = apply(ColumnName(name), srcColumn)
  
  def apply[A](name: ColumnName, srcColumn: ColumnExpr[A]): NamedColumnDef[A] = {
    new NamedColumnDef(name, srcColumn, None)
  }
  
  def apply(name: ColumnName): NamedColumnDef[String] = apply(name, name, name)
  
  
}
