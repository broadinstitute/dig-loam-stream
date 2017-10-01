package loamstream.util

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import lenthall.validation.ErrorOr.ErrorOr

import scala.collection.generic.CanBuildFrom
import scala.util.{Failure, Success, Try}

/** A container of an object or an error description */
sealed trait Shot[+A] {
  def get: A

  def message: String

  def map[B](f: A => B): Shot[B]

  def flatMap[B](f: A => Shot[B]): Shot[B]

  def asOpt: Option[A]

  def orElse[B >: A](alternative: => Shot[B]): Shot[B]

  def isEmpty: Boolean

  def nonEmpty: Boolean

  def flatten[A1](implicit evidence: A => Shot[A1]): Shot[A1] = this match {
    case m: Miss => m
    case Hit(shot) => shot
  }
  
  def isHit: Boolean
  def isMiss: Boolean

  def toErrorOr[AA >: A]: ErrorOr[AA]
}

/** A container of an object or an error description */
object Shot {
  def apply[A](f: => A): Shot[A] = fromTry(Try(f))

  def fromTry[A](myTry: Try[A]): Shot[A] =
    myTry match {
      case Success(a) => Hit(a)
      case Failure(ex) => Miss(Snag(ex))
    }

  def fromOption[A](option: Option[A], snag: => Snag): Shot[A] = option match {
    case Some(a) => Hit(a)
    case None => Miss(snag)
  }

  def notNull[A](a: A, snag: Snag): Shot[A] = Shot.fromOption(Option(a), snag)

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
  (shots: M[Shot[E]])(implicit cbf: CanBuildFrom[M[Shot[E]], E, M[E]]): Shot[M[E]] = {

    val misses = shots.collect { case miss: Miss => miss }

    if (misses.isEmpty) {
      val builder = cbf(shots)

      builder ++= shots.collect { case Hit(h) => h }

      Hit(builder.result())
    } else {
      Miss(misses.map(_.snag).reduce(_ ++ _))
    }
  }
  
  def combinedMiss(iter: Iterable[Shot[_]]): Miss = Miss(Snag(iter.collect { case Miss(snag) => snag }.toSeq))

  def findHit[A, B](items: Iterable[A], shooter: A => Shot[B]): Shot[B] = {
    if(items.isEmpty) { Miss("List of items is empty.") }
    else {
      val shots = items.map(shooter)
    
      val misses = shots.takeWhile(_.isMiss).collect { case miss: Miss => miss }
    
      val hitOpt = shots.dropWhile(_.isMiss).headOption.collect { case hit @ Hit(_) => hit }
    
      hitOpt match {
        case Some(hit) => hit
        case None => combinedMiss(misses)
      }
    }
  }
}

final case class Hit[+A](value: A) extends Shot[A] {
  override val get: A = value

  override def message: String = s"Hit: got '$value'"

  override def map[B](f: (A) => B): Shot[B] = Shot(f(value))

  override def flatMap[B](f: (A) => Shot[B]): Shot[B] = Shot(f(value)).flatten

  override def asOpt: Option[A] = Some(value)

  override def orElse[B >: A](alternative: => Shot[B]): Shot[B] = this

  override val isEmpty: Boolean = false

  override val nonEmpty: Boolean = true
  
  override val isHit: Boolean = true
  
  override val isMiss: Boolean = false

  override def toErrorOr[AA >: A]: Valid[AA] = Valid(value)
}

object Miss {
  def apply(message: String): Miss = Miss(SnagMessage(message))
}

final case class Miss(snag: Snag) extends Shot[Nothing] {
  override def get: Nothing = throw new NoSuchElementException(s"No such element: ${snag.message}")

  override def message: String = s"Miss: '${snag.message}'"

  override def map[B](f: (Nothing) => B): Shot[B] = Miss(snag)

  override val asOpt: None.type = None

  override def flatMap[B](f: (Nothing) => Shot[B]): Shot[B] = this

  override def orElse[B >: Nothing](alternative: => Shot[B]): Shot[B] = alternative match {
    case hit: Hit[B] => hit
    case Miss(altSnag) => Miss(snag ++ altSnag)
  }

  override val isEmpty: Boolean = true

  override val nonEmpty: Boolean = false
  
  override val isHit: Boolean = false
  
  override val isMiss: Boolean = true

  override def toErrorOr[AA]: Invalid[NonEmptyList[String]] =
    Invalid(NonEmptyList(snag.toString, snag.children.map(_.toString).toList))
}