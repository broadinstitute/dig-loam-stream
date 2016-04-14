package loamstream.util

import scala.util.Failure

/**
 * @author clint
 * date: Mar 10, 2016
 */
object Tries {
  def failure[T](message: String): Failure[T] = Failure(new Exception(message))
}