package loamstream.loam.intake

import org.apache.commons.csv.CSVRecord


/**
 * @author clint
 * Dec 17, 2019
 */
sealed trait ColumnExpr[A] extends RowParser[A] {
  def eval(row: CSVRecord): A
  
  final def render(row: CSVRecord): String = eval(row).toString
  
  final def map[B](f: A => B): ColumnExpr[B] = this ~> f
  final def ~>[B](f: A => B): ColumnExpr[B] = MappedColumnExpr(f, this)
  
  final def flatten(row: CSVRecord)(implicit ev: A =:= String): ColumnName = ColumnName(eval(row))
  
  final override def apply(row: CSVRecord): A = eval(row)
}

final case class ColumnName(name: String) extends ColumnExpr[String] {
  override def eval(row: CSVRecord): String = row.get(name)
}

object ColumnName {
  implicit final class ColumnNameOps(val s: String) extends AnyVal {
    def asColumnName: ColumnName = ColumnName(s)
  }
}

final case class MappedColumnExpr[A, B](f: A => B, dependsOn: ColumnExpr[A]) extends ColumnExpr[B] {
  override def eval(row: CSVRecord): B = f(dependsOn.eval(row))
}
