package loamstream.util

import scala.util.Failure
import scala.util.Try
import scala.collection.BuildFrom
import scala.util.Success

/**
 * @author clint
 * date: Mar 10, 2016
 */
object Tries {
  val defaultExceptionMaker: String => Exception = new Exception(_)
  
  def failure[T](message: String, makeException: String => Exception = defaultExceptionMaker): Failure[T] = {
    Failure(makeException(message))
  }
  
  import scala.language.higherKinds

  /**
    * NB: Use CanBuildFrom magic to make sure that the "best" type is returned, based on the
    * type passed in.  This allows Seq[Try[A]] => Try[Seq[A]], Set[Try[A]] => Shot[Try[A]],
    * Vector[Try[A]] => Try[Vector[A]], etc, etc, etc.
    *
    * Given a type T[A] <: Iterable[A],
    * and given a T[Try[A]], returns a Try[T[A]] such that:
    * If the input contains all Successes, returns a Success wrapping a T containing all the values of the input Tries.
    * If the input contains any Failures, returns the FIRST Failure encountered.
    */
  def sequence[E, M[X] <: Iterable[X]]
  (attempts: M[Try[E]])(implicit bf: BuildFrom[M[Try[E]], E, M[E]]): Try[M[E]] = {

    val firstFailureOpt = attempts.collectFirst { case f @ Failure(_) => f }

    firstFailureOpt match {
      case Some(Failure(e)) => Failure(e)
      case None => Try {
        val builder = bf.newBuilder(attempts)

        builder ++= attempts.collect { case Success(h) => h }
  
        builder.result()
      }
    }
  }
}
