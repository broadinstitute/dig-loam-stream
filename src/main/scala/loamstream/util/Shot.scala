package loamstream.util

import loamstream.util.ShotCombinators.Shots2

import scala.util.{Failure, Success, Try}

/**
  * LoamStream
  * Created by oliverr on 11/17/2015.
  */
sealed trait Shot[+A] {
  def get: A

  def map[B](f: A => B): Shot[B]

  def flatMap[B](f: A => Shot[B]): Shot[B]

  def asOpt: Option[A]

  def orElse[B >: A](alternative: => Shot[B]): Shot[B]

  def and[B](shotB: Shot[B]): Shots2[A, B] = Shots2(this, shotB)
}

object Shot {
  def fromTry[A](myTry: Try[A]): Shot[A] =
    myTry match {
      case Success(a) => Hit(a)
      case Failure(ex) => Miss(Snag(ex))
    }

  def fromOption[A](option: Option[A], snag: => Snag): Shot[A] = option match {
    case Some(a) => Hit(a)
    case None => Miss(snag)
  }

  def notNull[A](value: A, snag: => Snag): Shot[A] = if (value != null) { // scalastyle:ignore null
    Hit(value)
  } else {
    Miss(snag)
  }
}

case class Hit[+A](value: A) extends Shot[A] {
  override val get: A = value

  override def map[B](f: (A) => B): Shot[B] = Hit(f(value))

  override def flatMap[B](f: (A) => Shot[B]): Shot[B] = {
    f(value) match {
      case Hit(valueB) => Hit(valueB)
      case miss: Miss => miss
    }
  }

  override def asOpt: Option[A] = Some(value)

  override def orElse[B >: A](alternative: => Shot[B]): Shot[B] = this
}

object Miss {
  def apply(message: String): Miss = Miss(SnagMessage(message))
}

case class Miss(snag: Snag) extends Shot[Nothing] {
  override def get: Nothing = throw new NoSuchElementException(s"No such element: ${snag.message}")

  override def map[B](f: (Nothing) => B): Shot[B] = Miss(snag)

  override val asOpt = None

  override def flatMap[B](f: (Nothing) => Shot[B]): Shot[B] = this

  override def orElse[B >: Nothing](alternative: => Shot[B]): Shot[B] = alternative match {
    case hit: Hit[B] => hit
    case Miss(altSnag) => Miss(snag ++ altSnag)
  }
}