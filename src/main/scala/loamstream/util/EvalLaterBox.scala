package loamstream.util

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.reflect.runtime.universe.{Type, TypeTag}

/** Stores a by-name argument to evaluate later on demand */
class EvalLaterBox[T](expr: => T, val typeBox: TypeBox[T]) {
  /** Evaluates the expression */
  def eval: T = expr

  /** The type of the expression */
  def tpe: Type = typeBox.tpe

  /** Evaluates the expression to a Try */
  def evalTry: Try[T] = Try(expr)

  /** Evaluates the expression to a Shot */
  def evalShot: Shot[T] = Shot.fromTry(evalTry)

  /** Evaluates the expression asynchronously to a Future */
  def evalFuture(implicit ec: ExecutionContext): Future[T] = Future(expr)

  /** Returns new EvalLaterBox with expression prepended before this one's expression */
  def prepend(preExpr: => Any): EvalLaterBox[T] = EvalLaterBox.forType[T](typeBox)({
    preExpr
    expr
  })

  /** Returns new EvalLaterBox with expression appended after this one's expression */
  def append(postExpr: => Any): EvalLaterBox[T] = EvalLaterBox.forType[T](typeBox)({
    val result = expr
    postExpr
    result
  })

  /** Returns new EvalLaterBox with given expression wrapped around this one's expression  */
  def wrap(preExpr: => Any, postExpr: => Any): EvalLaterBox[T] = EvalLaterBox.forType[T](typeBox)({
    preExpr
    val result = expr
    postExpr
    result
  })
}

object EvalLaterBox {
  def forType[T](typeBox: TypeBox[T])(expr: => T): EvalLaterBox[T] = new EvalLaterBox[T](expr, typeBox)

  def apply[T: TypeTag](expr: => T): EvalLaterBox[T] = EvalLaterBox.forType[T](TypeBox.of[T])(expr)
}