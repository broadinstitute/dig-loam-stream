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
  
  object Implicits {
    final implicit class BooleanOptionOps(val o: Option[Boolean]) extends AnyVal {
      def orFalse: Boolean = o match {
        case Some(b) => b
        case _ => false
      }
      
      def orTrue: Boolean = o match {
        case Some(b) => b
        case _ => true
      }
    }
  }
}
