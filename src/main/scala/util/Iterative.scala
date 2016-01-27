package util

import util.Iterative.{Empty, FilteredIterative}

/**
  * LoamStream
  * Created by oliverr on 1/27/2016.
  */
object Iterative {

  trait SizePredicting[+A] extends Iterative[A] {
    def predictedSize: Int
  }

  object Empty extends SizePredicting[Nothing] {
    override val predictedSize: Int = 0

    override def tail: Iterative[Nothing] = Empty

    override def headOpt: Option[Nothing] = None

    override def head: Nothing = throw new NoSuchElementException("No more elements in this iterative.")

    override def empty: Boolean = true

    override def nonEmpty: Boolean = false
  }

  case class SetBased[A](set: Set[A]) extends SizePredicting[A] {
    def empty: Boolean = set.isEmpty

    def nonEmpty: Boolean = set.nonEmpty

    override def predictedSize: Int = set.size

    def tail: Iterative[A] = if (set.nonEmpty) SetBased(set - set.head) else Empty

    def headOpt: Option[A] = set.headOption

    def head: A = set.head

    override def filter(p: (A) => Boolean): Iterative[A] = SetBased(set.filter(p))

    override def toSeq: Seq[A] = set.toSeq
  }

  case class FilteredIterative[A](head: A, tail: Iterative[A], p: (A) => Boolean) extends Iterative[A] {
    override def empty: Boolean = false

    override def nonEmpty: Boolean = true

    override def headOpt: Option[A] = Some(head)
  }

}

trait Iterative[+A] {
  def empty: Boolean

  def nonEmpty: Boolean

  def head: A

  def headOpt: Option[A]

  def tail: Iterative[A]

  def seek(p: (A) => Boolean): Option[(A, Iterative[A])] =
    if (empty) None else if (p(head)) Some(head, tail) else tail.seek(p)

  def filter(p: (A) => Boolean): Iterative[A] =
    seek(p) match {
      case None => Empty
      case Some(sought) => FilteredIterative(sought._1, sought._2, p)
    }

  def toSeq: Seq[A] = if (empty) Seq.empty else head +: tail.toSeq

}
