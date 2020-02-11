package loamstream.loam.intake

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag
import scala.util.matching.Regex

/**
 * @author clint
 * Dec 17, 2019
 */
sealed abstract class ColumnExpr[A : TypeTag] extends RowParser[A] {
  def eval(row: CsvRow): A
  
  def isDefinedAt(row: CsvRow): Boolean = true
  
  final def dataType: DataType = DataType.fromTypeTag(typeTag[A])
  
  final def render(row: CsvRow): String = eval(row).toString
  
  final def map[B: TypeTag](f: A => B): ColumnExpr[B] = MappedColumnExpr(f, this)
  final def |>[B: TypeTag](f: A => B): ColumnExpr[B] = this.map(f)
  
  final def flatMap[B: TypeTag](f: A => ColumnExpr[B]): ColumnExpr[B] = FlatMappedColumnExpr(f, this)
  
  final override def apply(row: CsvRow): A = eval(row)
  
  import ColumnExpr._
  
  def asString: ColumnExpr[String] = this.map(_.toString)
  
  final def asInt(implicit ev: ConvertableToNumber[A]): ColumnExpr[Int] = this.map(ev.toInt(_))
  
  final def asLong(implicit ev: ConvertableToNumber[A]): ColumnExpr[Long] = this.map(ev.toLong(_))
  
  final def asDouble(implicit ev: ConvertableToNumber[A]): ColumnExpr[Double] = this.map(ev.toDouble(_))
  
  final def asUpperCase(implicit ev: A =:= String): ColumnExpr[String] = this.map(a => ev(a).toUpperCase)
  
  final def -(expr: ColumnExpr[A])(implicit ev: Numeric[A]): ColumnExpr[A] = arithmeticOp(expr)(_.minus)
    
  final def +(expr: ColumnExpr[A])(implicit ev: Numeric[A]): ColumnExpr[A] = arithmeticOp(expr)(_.plus)
    
  final def *(expr: ColumnExpr[A])(implicit ev: Numeric[A]): ColumnExpr[A] = arithmeticOp(expr)(_.times)
  
  final def /(expr: ColumnExpr[A])(implicit ev: Fractional[A]): ColumnExpr[A] = {
    val fractional = implicitly[Fractional[A]]
    
    ColumnExpr.lift2(fractional.div).apply(this, expr)
  }
  
  final def unary_-(implicit ev: Numeric[A]): ColumnExpr[A] = {
    this.map(a => implicitly[Numeric[A]].mkNumericOps(a).unary_-())
  }
  
  final def negate(implicit ev: Numeric[A]): ColumnExpr[A] = this.unary_-
  
  final def complement(implicit ev: Numeric[A]): ColumnExpr[A] = {
    this.map { a => ev.minus(ev.one, a) }
  }
  
  final def complementIf(p: A => Boolean)(implicit ev: Numeric[A]): ColumnExpr[A] = {
    this.map { a => if(p(a)) ev.minus(ev.one, a) else a }
  }
  
  final def exp(implicit ev: A =:= Double): ColumnExpr[Double] = this.map(a => scala.math.exp(ev(a)))
    
  final def mapRegex[B: TypeTag](regexString: String)(f: Seq[String] => B): ColumnExpr[B] = mapRegex(regexString.r)(f)
  
  final def mapRegex[B: TypeTag](regex: Regex)(f: Seq[String] => B): ColumnExpr[B] = {
    this.asString.map { columnValue => 
      regex.unapplySeq(columnValue).map(f).getOrElse {
        throw new Exception(s"Column value '${columnValue}' doesn't match regex $regex")
      }
    }
  }
  
  final def orElse[B: TypeTag](default: => B)(implicit ev: A =:= Option[B]): ColumnExpr[B] = {
    this.map(a => ev(a).getOrElse(default))
  }
  
  final def matches(regex: String): RowPredicate = matches(regex.r)
  
  final def matches(regex: Regex): RowPredicate = this.asString.map(regex.pattern.matcher(_).matches)
  
  final def ===(rhs: A): RowPredicate = this.map(_ == rhs)
  final def !==(rhs: A): RowPredicate = this.map(_ != rhs)
  
  private def arithmeticOp(
      expr: ColumnExpr[A])(op: Numeric[A] => (A, A) => A)(implicit ev: Numeric[A]): ColumnExpr[A] = {

    val f = op(implicitly[Numeric[A]])
    
    ColumnExpr.lift2(f).apply(this, expr)
  }
}

object ColumnExpr {
  def fromRowParser[A: TypeTag](rowParser: RowParser[A]): ColumnExpr[A] = new ColumnExpr[A] {
    override def eval(row: CsvRow): A = rowParser(row)
  }
  
  def fromPartialRowParser[A: TypeTag](rowParser: PartialRowParser[A]): ColumnExpr[A] = new PartialColumnExpr(rowParser)
  
  implicit final class ExprOps[A](val a: A) extends AnyVal {
    def +(rhs: ColumnExpr[A])(implicit ev: Numeric[A], tt: TypeTag[A]): ColumnExpr[A] = LiteralColumnExpr(a) + rhs
    def -(rhs: ColumnExpr[A])(implicit ev: Numeric[A], tt: TypeTag[A]): ColumnExpr[A] = LiteralColumnExpr(a) - rhs
    def *(rhs: ColumnExpr[A])(implicit ev: Numeric[A], tt: TypeTag[A]): ColumnExpr[A] = LiteralColumnExpr(a) * rhs
    def /(rhs: ColumnExpr[A])(implicit ev: Fractional[A], tt: TypeTag[A]): ColumnExpr[A] = LiteralColumnExpr(a) / rhs
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
  
  implicit def lift[A: TypeTag, B: TypeTag](f: A => B): ColumnExpr[A] => ColumnExpr[B] = _.map(f)
  
  implicit def lift2[A: TypeTag, B: TypeTag, C: TypeTag]
      (f: (A, B) => C): (ColumnExpr[A], ColumnExpr[B]) => ColumnExpr[C] = { (lhsExpr, rhsExpr) =>
    for {
      lhs <- lhsExpr
      rhs <- rhsExpr
    } yield f(lhs, rhs)
  }
}

final class PartialColumnExpr[A: TypeTag](pf: PartialRowParser[A]) extends ColumnExpr[A] {
  override def eval(row: CsvRow): A = pf(row)
  
  override def isDefinedAt(row: CsvRow): Boolean = pf.isDefinedAt(row)
}

final case class LiteralColumnExpr[A: TypeTag](value: A) extends ColumnExpr[A] {
  override def toString: String = value.toString 
  
  override def eval(ignored: CsvRow): A = value
  
  override def asString: ColumnExpr[String] = value match {
    case s: String => this.asInstanceOf[LiteralColumnExpr[String]]
    case _ => LiteralColumnExpr(value.toString)
  }
}

final case class ColumnName(name: String) extends ColumnExpr[String] {
  override def eval(row: CsvRow): String = {
    val value = row.getFieldByName(name)
    
    require(value != null, s"Field named '$name' not found in row $row") 
    
    value
  }
  
  override def asString: ColumnExpr[String] = this
}

final case class MappedColumnExpr[A: TypeTag, B: TypeTag](f: A => B, dependsOn: ColumnExpr[A]) extends ColumnExpr[B] {
  override def eval(row: CsvRow): B = f(dependsOn.eval(row))
}

final case class FlatMappedColumnExpr[A: TypeTag, B: TypeTag](f: A => ColumnExpr[B], dependsOn: ColumnExpr[A]) extends ColumnExpr[B] {
  override def eval(row: CsvRow): B = f(dependsOn.eval(row)).eval(row)
}
