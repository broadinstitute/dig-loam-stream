package loamstream.util.shot

import loamstream.util.snag.{Snag, SnagMessage}

import scala.util.{Failure, Success, Try}

/**
  * LoamStream
  * Created by oliverr on 11/17/2015.
  */
object Shot {
  def fromTry[A](myTry: Try[A]): Shot[A] =
    myTry match {
      case Success(a) => Hit(a)
      case Failure(ex) => Miss(Snag(ex))
    }
}

sealed trait Shot[+A] {
  def get: A

  def map[B](f: A => B): Shot[B]

  def flatMap[B](f: A => Shot[B]): Shot[B]

  def asOpt: Option[A]
}

case class Hit[+A](value: A) extends Shot[A] {
  override val get = value

  override def map[B](f: (A) => B): Shot[B] = Hit(f(value))

  override def flatMap[B](f: (A) => Shot[B]): Shot[B] = {
    f(value) match {
      case Hit(valueB) => Hit(valueB)
      case miss: Miss => miss
    }
  }

  override def asOpt: Option[A] = Some(value)
}

object Miss {
  def apply(message: String): Miss = Miss(SnagMessage(message))
}

case class Miss(snag: Snag) extends Shot[Nothing] {
  override def get: Nothing = throw new NoSuchElementException("No such element: " + snag.message)

  override def map[B](f: (Nothing) => B): Shot[B] = Miss(snag)

  override val asOpt = None

  override def flatMap[B](f: (Nothing) => Shot[B]): Shot[B] = this

}