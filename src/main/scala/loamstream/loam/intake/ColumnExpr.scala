package loamstream.loam.intake

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag
import scala.util.matching.Regex
import scala.util.control.NonFatal
import loamstream.util.Sequence
import scala.util.Try
import scala.util.Success
import loamstream.util.Iterators

/**
 * @author clint
 * Dec 17, 2019
 */
sealed abstract class ColumnExpr[A : TypeTag] extends 
    ColumnExpr.ArithmeticOps[A] with ColumnExpr.TypeOps[A] with ColumnExpr.BooleanOps[A] with DataRowParser[A] {

  protected def eval(row: DataRow): A
  
  protected[intake] def tpe: TypeTag[A] = implicitly[TypeTag[A]]
  
  final override def apply(row: DataRow): A = {
    try { eval(row) }
    catch {
      case e: CsvProcessingException => throw e
      case NonFatal(e) => throw newCsvProcessingException(row, e)
    }
  }
  
  protected def newCsvProcessingException(row: DataRow, cause: Throwable) = {
    new CsvProcessingException(
        s"Error processing record number ${row.recordNumber} with expr ${this} (${getClass.getName}); row is '$row':", 
        row, 
        cause)
  }
  
  final def ~>(name: String): NamedColumnDef[A] = NamedColumnDef(name, this)
  final def ~>(name: ColumnName): NamedColumnDef[A] = NamedColumnDef(name, this)
  
  def applyOpt(row: DataRow): Option[A] = Try(apply(row)).toOption 
  
  def isDefinedAt(row: DataRow): Boolean = true
  
  final def render(row: DataRow): String = eval(row).toString
  
  final def map[B: TypeTag](f: A => B): ColumnExpr[B] = MappedColumnExpr(f, this)
  
  final def flatMap[B: TypeTag](f: A => ColumnExpr[B]): ColumnExpr[B] = FlatMappedColumnExpr(f, this)
  
  import ColumnExpr._
  
  def or(rhs: ColumnExpr[A]): OrColumnExpr[A] = OrColumnExpr(this, rhs)
  
  def asOption: ColumnExpr[Option[A]] = OptionalExpr(this)
  
  def asString: ColumnExpr[String] = {
    if(isStringExpr) { this.asInstanceOf[ColumnExpr[String]] }
    else { this.map(_.toString) }
  }
  
  final def asInt(implicit ev: ConvertableToNumber[A]): ColumnExpr[Int] = this.map(ev.toInt(_))
  
  final def asLong(implicit ev: ConvertableToNumber[A]): ColumnExpr[Long] = this.map(ev.toLong(_))
  
  final def asDouble(implicit ev: ConvertableToNumber[A]): ColumnExpr[Double] = this.map(ev.toDouble(_))
  
  final def asUpperCase(implicit ev: A =:= String): ColumnExpr[String] = this.map(a => ev(a).toUpperCase)
  
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
  
  final def matches(regex: String): DataRowPredicate = matches(regex.r)
  
  final def matches(regex: Regex): DataRowPredicate = this.asString.map(regex.pattern.matcher(_).matches)
  
  final def trim(implicit ev: A =:= String): ColumnExpr[String] = this.map(_.trim)
  
  final def isEmpty(implicit ev: A =:= String): DataRowPredicate = this.map(_.isEmpty)
  
  final def isEmptyIgnoreWhitespace(implicit ev: A =:= String): DataRowPredicate = this.map(_.trim.isEmpty)
}

object ColumnExpr {
  object String {
    def unapply(expr: ColumnExpr[_]): Option[ColumnExpr[String]] = {
      if(expr.isStringExpr) Some(expr.asInstanceOf[ColumnExpr[String]]) else None
    }
  }
  
  object Double {
    def unapply(expr: ColumnExpr[_]): Option[ColumnExpr[Double]] = {
      if(expr.isDoubleExpr) Some(expr.asInstanceOf[ColumnExpr[Double]]) else None
    }
  }
  
  trait BooleanOps[A] { self: ColumnExpr[A] =>
    final def ===(rhs: A): DataRowPredicate = this.map(_ == rhs)
    final def !==(rhs: A): DataRowPredicate = this.map(_ != rhs) //scalastyle:ignore method.name
    
    final def <(rhs: A)(implicit ev: Ordering[A]): DataRowPredicate = this.map(lhs => ev.lt(lhs, rhs))
    final def <=(rhs: A)(implicit ev: Ordering[A]): DataRowPredicate = this.map(lhs => ev.lteq(lhs, rhs))
    final def >(rhs: A)(implicit ev: Ordering[A]): DataRowPredicate = this.map(lhs => ev.gt(lhs, rhs))
    final def >=(rhs: A)(implicit ev: Ordering[A]): DataRowPredicate = this.map(lhs => ev.gteq(lhs, rhs))
  }
  
  trait TypeOps[A] { self: ColumnExpr[A] =>
    final protected[intake] def isStringExpr: Boolean = tpe == typeTag[String]
    final protected[intake] def isIntExpr: Boolean = tpe == typeTag[Int]
    final protected[intake] def isLongExpr: Boolean = tpe == typeTag[Long]
    final protected[intake] def isDoubleExpr: Boolean = tpe == typeTag[Double]
  }
  
  abstract class ArithmeticOps[A : TypeTag] { self: ColumnExpr[A] =>
    final def -(rhs: ColumnExpr[A])(implicit ev: Numeric[A]): ColumnExpr[A] = arithmeticOp(this, rhs)(_.minus)
    final def -(rhs: A)(implicit ev: Numeric[A]): ColumnExpr[A] = this - LiteralColumnExpr(rhs)
      
    final def +(rhs: ColumnExpr[A])(implicit ev: Numeric[A]): ColumnExpr[A] = arithmeticOp(this, rhs)(_.plus)
    final def +(rhs: A)(implicit ev: Numeric[A]): ColumnExpr[A] = this + LiteralColumnExpr(rhs)
      
    final def *(rhs: ColumnExpr[A])(implicit ev: Numeric[A]): ColumnExpr[A] = arithmeticOp(this, rhs)(_.times)
    final def *(rhs: A)(implicit ev: Numeric[A]): ColumnExpr[A] = this * LiteralColumnExpr(rhs)
    
    final def /(rhs: ColumnExpr[A])(implicit ev: Fractional[A]): ColumnExpr[A] = {
      val fractional = implicitly[Fractional[A]]
      
      ColumnExpr.lift2(fractional.div).apply(this, rhs)
    }
    
    final def /(rhs: A)(implicit ev: Fractional[A]): ColumnExpr[A] = this / LiteralColumnExpr(rhs)
    
    final def unary_-(implicit ev: Numeric[A]): ColumnExpr[A] = {
      this.map(a => implicitly[Numeric[A]].mkNumericOps(a).unary_-())
    }
    
    final def negate(implicit ev: Numeric[A]): ColumnExpr[A] = this.unary_-
  }
  
  def fromRowParser[A: TypeTag](rowParser: DataRowParser[A]): ColumnExpr[A] = new ColumnExpr[A] {
    override def eval(row: DataRow): A = rowParser(row)
  }
  
  def fromPartialRowParser[A: TypeTag](rowParser: PartialDataRowParser[A]): ColumnExpr[A] = new PartialColumnExpr(rowParser)
  
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
  
  implicit def numericTypesAreConvertableToNumbers[A](implicit ev: Numeric[A]): ConvertableToNumber[A] = {
    new ConvertableToNumber[A] {
      override def toInt(a: A): Int = ev.toInt(a)
      override def toLong(a: A): Long = ev.toLong(a)
      override def toFloat(a: A): Float = ev.toFloat(a)
      override def toDouble(a: A): Double = ev.toDouble(a)
    }
  }
  
  def asDouble(column: ColumnExpr[_]): ColumnExpr[Double] = column match {
    case ColumnExpr.Double(expr) => expr
    case ColumnExpr.String(expr) => expr.asDouble
    case _ => column.asString.asDouble
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
      
  private def arithmeticOp[A : TypeTag](
      lhs: ColumnExpr[A],
      rhs: ColumnExpr[A])(op: Numeric[A] => (A, A) => A)(implicit ev: Numeric[A]): ColumnExpr[A] = {

    val f = op(implicitly[Numeric[A]])
    
    lift2(f).apply(lhs, rhs)
  }
}

final class PartialColumnExpr[A: TypeTag](pf: PartialDataRowParser[A]) extends ColumnExpr[A] {
  override def eval(row: DataRow): A = pf(row)
  
  override def isDefinedAt(row: DataRow): Boolean = pf.isDefinedAt(row)
}

final case class LiteralColumnExpr[A: TypeTag](value: A) extends ColumnExpr[A] {
  override def toString: String = value.toString 
  
  override def eval(ignored: DataRow): A = value
  
  override def asString: ColumnExpr[String] = value match {
    case s: String => this.asInstanceOf[LiteralColumnExpr[String]]
    case _ => LiteralColumnExpr(value.toString)
  }
}

final case class ColumnName(name: String) extends ColumnExpr[String] {
  override def toString: String = s"${getClass.getSimpleName}(${name})"
  
  override def eval(row: DataRow): String = {
    val value = row.getFieldByName(name)
    
    require(value != null, s"Field named '$name' not found in record number ${row.recordNumber} (row $row)") 
    
    value
  }
  
  private[intake] val index: Int = ColumnName.nextColumnIndex()
  
  override def asString: ColumnExpr[String] = this
  
  def mapName(f: String => String): ColumnName = copy(name = f(name))
}

object ColumnName {
  private[this] val indices: Sequence[Int] = Sequence()
  
  private[intake] def nextColumnIndex(): Int = indices.next()
}

final case class MappedColumnExpr[A: TypeTag, B: TypeTag](f: A => B, dependsOn: ColumnExpr[A]) extends ColumnExpr[B] {
  override def toString: String = s"${getClass.getSimpleName}(${f}, ${dependsOn})"
  
  override protected def eval(row: DataRow): B = f(dependsOn(row))
}

final case class FlatMappedColumnExpr[A: TypeTag, B: TypeTag](
    f: A => ColumnExpr[B], 
    dependsOn: ColumnExpr[A]) extends ColumnExpr[B] {
  
  override def toString: String = s"${getClass.getSimpleName}(${f}, ${dependsOn})"
  
  override protected def eval(row: DataRow): B = f(dependsOn(row)).apply(row)
}

final case class OrColumnExpr[A : TypeTag](lhs: ColumnExpr[A], rhs: ColumnExpr[A]) extends ColumnExpr[A] {
  override def toString: String = s"${getClass.getSimpleName}(${lhs}, ${rhs})"
  
  override protected def eval(row: DataRow): A = Try(lhs(row)).getOrElse(rhs(row))
}

final case class OptionalExpr[A : TypeTag](dependsOn: ColumnExpr[A]) extends ColumnExpr[Option[A]] {
  override def toString: String = s"${getClass.getSimpleName}(${dependsOn})"
  
  override protected def eval(row: DataRow): Option[A] = dependsOn.applyOpt(row)
}
