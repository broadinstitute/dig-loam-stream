package loamstream.loam.intake

import loamstream.util.Sequence
import scala.reflect.runtime.universe.TypeTag

/**
 * @author clint
 * Oct 15, 2020
 */
trait ColumnDef[A] extends (VariantRow.Tagged => A) {
  def map[B: TypeTag](f: A => B): ColumnDef[B] = MappedColumnDef(this, f)
}

object ColumnDef {
  def combine[A, B, C](
      lhsDef: ColumnDef[A], 
      rhsDef: ColumnDef[B])(op: (A, B) => C): ColumnDef[C] = {
    
    CombinedColumnDef(lhsDef, rhsDef, op)
  }
}

trait HandlesFlipsColumnDef[A] extends ColumnDef[A] {
  def expr: ColumnExpr[A]
  
  def exprWhenFlipped: Option[ColumnExpr[A]]
  
  override def apply(row: VariantRow.Tagged): A = {
    val exprToInvoke: ColumnExpr[A] = {
      def whenFlipped: ColumnExpr[A] = exprWhenFlipped match {
        case Some(e) => e
        case None => expr
      }
      
      if(row.isFlipped) whenFlipped else expr
    }
    
    exprToInvoke(row)
  }
}

final case class MarkerColumnDef(
    name: ColumnName,
    expr: ColumnExpr[Variant]) extends (DataRow => Variant) { self =>
  
  override def apply(row: DataRow): Variant = expr.apply(row)
}

final case class AnonColumnDef[A](
  expr: ColumnExpr[A],
  exprWhenFlipped: Option[ColumnExpr[A]] = None) extends HandlesFlipsColumnDef[A]

object AnonColumnDef {
  def apply[A](expr: ColumnExpr[A], exprWhenFlipped: ColumnExpr[A]): AnonColumnDef[A] = {
    new AnonColumnDef(expr, Option(exprWhenFlipped))
  }
}

final case class MappedColumnDef[A, B](
    delegate: ColumnDef[A],
    f: A => B) extends ColumnDef[B] {
  
  override def apply(row: VariantRow.Tagged): B = f(delegate(row))
}

final case class CombinedColumnDef[A, B, C](
    lhsDef: ColumnDef[A], 
    rhsDef: ColumnDef[B],
    op: (A, B) => C) extends ColumnDef[C] {
  
  override def apply(row: VariantRow.Tagged): C = op(lhsDef(row), rhsDef(row))
}
