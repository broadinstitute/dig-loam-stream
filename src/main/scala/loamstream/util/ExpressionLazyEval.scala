package loamstream.util

/** Stores an expression to evaluate later */
class ExpressionLazyEval[T](expr: => T) {
  def eval: T = expr
}
