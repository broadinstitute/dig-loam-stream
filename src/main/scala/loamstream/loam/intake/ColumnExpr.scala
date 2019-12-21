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
  
  final def flatMap[B](f: A => ColumnExpr[B]): ColumnExpr[B] = FlatMappedColumnExpr(f, this)
  
  final def flatten(row: CSVRecord)(implicit ev: A =:= String): ColumnName = ColumnName(eval(row))
  
  final override def apply(row: CSVRecord): A = eval(row)
  
  import ColumnExpr._
  
  def asString: ColumnExpr[String] = this.map(_.toString)
  
  final def asInt(implicit ev: ConvertableToNumber[A]): ColumnExpr[Int] = this.map(ev.toInt(_))
  
  final def asDouble(implicit ev: ConvertableToNumber[A]): ColumnExpr[Double] = this.map(ev.toDouble(_))
  
  final def asUpperCase(implicit ev: A =:= String): ColumnExpr[String] = this.map(a => ev(a).toUpperCase)
  
  final def -(expr: ColumnExpr[A])(implicit ev: Numeric[A]): ColumnExpr[A] = arithmeticOp(expr)(_.minus)
    
  final def +(expr: ColumnExpr[A])(implicit ev: Numeric[A]): ColumnExpr[A] = arithmeticOp(expr)(_.plus)
    
  final def *(expr: ColumnExpr[A])(implicit ev: Numeric[A]): ColumnExpr[A] = arithmeticOp(expr)(_.times)
    
  final def unary_-(implicit ev: Numeric[A]): ColumnExpr[A] = {
    this.map(a => implicitly[Numeric[A]].mkNumericOps(a).unary_-())
  }
    
  private def arithmeticOp(
      expr: ColumnExpr[A])(op: Numeric[A] => (A, A) => A)(implicit ev: Numeric[A]): ColumnExpr[A] = {

    val f = op(implicitly[Numeric[A]])
    
    ColumnExpr.lift2(f).apply(this, expr)
  }
}

object ColumnExpr {
  implicit final class ExprOps[A](val a: A) extends AnyVal {
    def +(rhs: ColumnExpr[A])(implicit ev: Numeric[A]): ColumnExpr[A] = Literal(a) + rhs
    def -(rhs: ColumnExpr[A])(implicit ev: Numeric[A]): ColumnExpr[A] = Literal(a) - rhs
    def *(rhs: ColumnExpr[A])(implicit ev: Numeric[A]): ColumnExpr[A] = Literal(a) * rhs
  }
  
  trait ConvertableToNumber[A] {
    def toInt(a: A): Int
    def toLong(a: A): Long
    def toFloat(a: A): Float
    def toDouble(a: A): Double
  }
  
  implicit object StringsAreConvertableToNumbers extends ConvertableToNumber[String] {
    override def toInt(a: String): Int = a.toInt
    override def toLong(a: String): Long = a.toLong
    override def toFloat(a: String): Float = a.toFloat
    override def toDouble(a: String): Double = a.toDouble
  }
  
  implicit def NumericTypesAreConvertableToNumbers[A](implicit ev: Numeric[A]): ConvertableToNumber[A] = {
    new ConvertableToNumber[A] {
      override def toInt(a: A): Int = ev.toInt(a)
      override def toLong(a: A): Long = ev.toLong(a)
      override def toFloat(a: A): Float = ev.toFloat(a)
      override def toDouble(a: A): Double = ev.toDouble(a)
    }
  }
  
  import scala.language.implicitConversions
  
  implicit def lift[A, B](f: A => B): ColumnExpr[A] => ColumnExpr[B] = _.map(f)
  
  implicit def lift2[A, B, C](f: (A, B) => C): (ColumnExpr[A], ColumnExpr[B]) => ColumnExpr[C] = { (lhsExpr, rhsExpr) =>
    for {
      lhs <- lhsExpr
      rhs <- rhsExpr
    } yield f(lhs, rhs)
  }
}

final case class Literal[A](value: A) extends ColumnExpr[A] {
  override def eval(ignored: CSVRecord): A = value
  
  override def asString: ColumnExpr[String] = value match {
    case s: String => this.asInstanceOf[Literal[String]]
    case _ => Literal(value.toString)
  }
}

final case class ColumnName(name: String) extends ColumnExpr[String] {
  override def eval(row: CSVRecord): String = row.get(name)
  
  override def asString: ColumnExpr[String] = Literal(name)
}

final case class MappedColumnExpr[A, B](f: A => B, dependsOn: ColumnExpr[A]) extends ColumnExpr[B] {
  override def eval(row: CSVRecord): B = f(dependsOn.eval(row))
}

final case class FlatMappedColumnExpr[A, B](f: A => ColumnExpr[B], dependsOn: ColumnExpr[A]) extends ColumnExpr[B] {
  override def eval(row: CSVRecord): B = f(dependsOn.eval(row)).eval(row)
}
