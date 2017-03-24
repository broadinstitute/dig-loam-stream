package loamstream.util

import scala.util.Success
import scala.util.Try

/**
 * @author clint
 * Oct 17, 2016
 */
object Options {
  def toTry[A](o: Option[A])(
      ifNoneMessage: => String, 
      makeException: String => Exception = Tries.defaultExceptionMaker): Try[A] = o match {
    
    case Some(a) => Success(a)
    case None => Tries.failure(ifNoneMessage, makeException)
  }
}
