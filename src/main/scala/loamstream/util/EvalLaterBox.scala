package loamstream.util

import scala.concurrent.Future
import scala.util.Try

/** Stores a by-name argument to evaluate later on demand */
class EvalLaterBox[T](expr: => T) {
/** Evaluates the expression */
  def eval: T = expr

  /** Evaluates the expression to a Try */
  def evalTry: Try[T] = Try (expr)

  /** Evaluates the expression to a Shot */
  def evalShot: Shot[T] = Shot.fromTry(evalTry)

  /** Evaluates the expression asynchronously to a Future */
  def evalFuture: Future[T] = Future (expr)
}
