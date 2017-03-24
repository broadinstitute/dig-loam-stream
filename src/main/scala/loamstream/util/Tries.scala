package loamstream.util

import scala.util.Failure

/**
 * @author clint
 * date: Mar 10, 2016
 */
object Tries {
  val defaultExceptionMaker: String => Exception = new Exception(_)
  
  def failure[T](message: String, makeException: String => Exception = defaultExceptionMaker): Failure[T] = {
    Failure(makeException(message))
  }
}
