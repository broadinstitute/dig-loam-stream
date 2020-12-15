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
    implicit final class OptionOps[A](val o: Option[A]) extends AnyVal {
      def orElseFalse(implicit ev: A =:= Boolean): Boolean = o match {
        case Some(b) => b
        case None => false
      }
      
      def zip[B](ob: Option[B]): Option[(A, B)] = (o, ob) match {
        case (Some(a), Some(b)) => Some((a, b))
        case _ => None
      }
    }
  }
}
