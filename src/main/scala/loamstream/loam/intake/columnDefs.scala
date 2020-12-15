package loamstream.loam.intake

import loamstream.util.Sequence
import scala.reflect.runtime.universe.TypeTag

/**
 * @author clint
 * Oct 15, 2020
 */
trait ColumnDef[A] extends (CsvRow.Tagged => A) {
  
  override def apply(row: CsvRow.Tagged): A = {
    val exprToInvoke: ColumnExpr[A] = {
      def whenFlipped: ColumnExpr[A] = exprWhenFlipped match {
        case Some(e) => e
        case None => expr
      }
      
      if(row.isFlipped) whenFlipped else expr
    }
    
    exprToInvoke(row)
  }
  
  def expr: ColumnExpr[A]
  
  def exprWhenFlipped: Option[ColumnExpr[A]]
}

final case class MarkerColumnDef(
    name: ColumnName,
    expr: ColumnExpr[Variant]) extends (CsvRow => Variant) { self =>
  
  override def apply(row: CsvRow): Variant = expr.apply(row)
}

/**
 * @author clint
 * Dec 17, 2019
 */
final case class NamedColumnDef[A](
  name: ColumnName,
  expr: ColumnExpr[A],
  exprWhenFlipped: Option[ColumnExpr[A]]) extends ColumnDef[A] {
  
  override def toString: String = s"${getClass.getSimpleName}(${name}, ${expr}, ${exprWhenFlipped})"
}

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
