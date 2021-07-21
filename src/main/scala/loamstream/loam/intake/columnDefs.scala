package loamstream.loam.intake

import loamstream.util.Sequence
import scala.reflect.runtime.universe._

/**
 * @author clint
 * Oct 15, 2020
 */
abstract class ColumnDef[A: TypeTag] extends (VariantRow.Tagged => A) {
  private implicit def tuple2tt[A: TypeTag, B: TypeTag]: TypeTag[(A, B)] = typeTag[(A, B)]

  def map[B: TypeTag](f: A => B): ColumnDef[B] = MappedColumnDef(this, f)

  def flatMap[B: TypeTag](f: A => ColumnDef[B]): ColumnDef[B] = FlatMappedColumnDef(this, f)

  def zip[B: TypeTag](rhs: ColumnDef[B]): ColumnDef[(A, B)] = combine(rhs)(_ -> _)

  def combine[B: TypeTag, C: TypeTag](rhs: ColumnDef[B])(f: (A, B) => C): ColumnDef[C] = {
    CombinedColumnDef(this, rhs, f)
  }
}

object ColumnDef {
  def combine[A: TypeTag, B: TypeTag, C: TypeTag](
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

final case class AnonColumnDef[A: TypeTag](
  expr: ColumnExpr[A],
  exprWhenFlipped: Option[ColumnExpr[A]] = None) extends HandlesFlipsColumnDef[A]

object AnonColumnDef {
  def apply[A: TypeTag](expr: ColumnExpr[A], exprWhenFlipped: ColumnExpr[A]): AnonColumnDef[A] = {
    AnonColumnDef(expr, Option(exprWhenFlipped))
  }
}

final case class MappedColumnDef[A: TypeTag, B: TypeTag](
    delegate: ColumnDef[A],
    f: A => B) extends ColumnDef[B] {
  
  override def apply(row: VariantRow.Tagged): B = f(delegate(row))
}

final case class FlatMappedColumnDef[A: TypeTag, B: TypeTag](
    delegate: ColumnDef[A],
    f: A => ColumnDef[B]) extends ColumnDef[B] {
  
  override def apply(row: VariantRow.Tagged): B = f(delegate(row)).apply(row)
}

final case class CombinedColumnDef[A: TypeTag, B: TypeTag, C: TypeTag](
    lhsDef: ColumnDef[A], 
    rhsDef: ColumnDef[B],
    op: (A, B) => C) extends ColumnDef[C] {
  
  override def apply(row: VariantRow.Tagged): C = op(lhsDef(row), rhsDef(row))
}
