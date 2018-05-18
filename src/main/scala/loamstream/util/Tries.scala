package loamstream.util

import scala.util.Failure
import scala.util.Try
import scala.collection.generic.CanBuildFrom
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
    * LoamStream
    * Created by oliverr on 4/14/2016.
    *
    * NB: Now named sequence to match the general monadic pattern, and
    * specific methods like Future.sequence, etc.
    * NB: Use CanBuildFrom magic to make sure that the "best" type is returned, based on the
    * type passed in.  This allows Seq[Shot[A]] => Shot[Seq[A]], Set[Shot[A]] => Shot[Set[A]],
    * Vector[Shot[A]] => Shot[Vector[A]], etc, etc, etc.
    *
    * Given a type T[A] <: Traversable[A],
    * and given a T[Shot[A]], returns a Shot[T[A]] such that:
    * If the input contains all hits, returns a Hit wrapping a T containing all the values of the input Hits.
    * If the input contains any misses, returns a Miss wrapping the concatenation of all the input Miss's Snags.
    */
  def sequence[E, M[X] <: Traversable[X]]
  (attempts: M[Try[E]])(implicit cbf: CanBuildFrom[M[Try[E]], E, M[E]]): Try[M[E]] = {

    val firstFailureOpt = attempts.collectFirst { case f @ Failure(_) => f }

    firstFailureOpt match {
      case Some(Failure(e)) => Failure(e)
      case None => Try {
        val builder = cbf(attempts)

        builder ++= attempts.collect { case Success(h) => h }
  
        builder.result()
      }
    }
  }
}
