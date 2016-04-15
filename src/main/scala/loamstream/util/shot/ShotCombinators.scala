package loamstream.util.shot

import loamstream.util.snag.Snag

/**
  * LoamStream
  * Created by oliverr on 4/7/2016.
  */
object ShotCombinators {

  trait ShotsN {
    def and[TN](_n: Shot[TN]): ShotsN
  }

  def combinedMiss[T](iter: Iterator[T]): Miss = Miss(Snag(iter.collect({ case Miss(snag) => snag }).toSeq))

  case class Shots1[+T1](_1: Shot[T1]) extends Product1[Shot[T1]] with ShotsN {
    override def and[T2](_2: Shot[T2]): Shots2[T1, T2] = Shots2(_1, _2)

    def get: Shot[T1] = _1 match {
      case Hit(e1) => _1
      case Miss(snag) => Miss(snag)
    }

    def apply[R](f: (T1) => R): Shot[R] = _1 match {
      case Hit(e1) => Hit(f(e1))
      case Miss(snag) => Miss(snag)
    }
  }

  case class Shots2[+T1, +T2](_1: Shot[T1], _2: Shot[T2]) extends Product2[Shot[T1], Shot[T2]] with ShotsN {
    override def and[T3](_3: Shot[T3]): Shots3[T1, T2, T3] = Shots3(_1, _2, _3)

    def get: Shot[(T1, T2)] = (_1, _2) match {
      case (Hit(e1), Hit(e2)) => Hit((e1, e2))
      case _ => combinedMiss(productIterator)
    }

    def apply[R](f: (T1, T2) => R): Shot[R] = (_1, _2) match {
      case (Hit(e1), Hit(e2)) => Hit(f(e1, e2))
      case _ => combinedMiss(productIterator)
    }
  }

  case class Shots3[+T1, +T2, +T3](_1: Shot[T1], _2: Shot[T2], _3: Shot[T3])
    extends Product3[Shot[T1], Shot[T2], Shot[T3]] with ShotsN {
    override def and[T4](_4: Shot[T4]): Shots4[T1, T2, T3, T4] = Shots4(_1, _2, _3, _4)

    def get: Shot[(T1, T2, T3)] = (_1, _2, _3) match {
      case (Hit(e1), Hit(e2), Hit(e3)) => Hit((e1, e2, e3))
      case _ => combinedMiss(productIterator)
    }

    def apply[R](f: (T1, T2, T3) => R): Shot[R] = (_1, _2, _3) match {
      case (Hit(e1), Hit(e2), Hit(e3)) => Hit(f(e1, e2, e3))
      case _ => combinedMiss(productIterator)
    }
  }

  case class Shots4[+T1, +T2, +T3, +T4](_1: Shot[T1], _2: Shot[T2], _3: Shot[T3], _4: Shot[T4])
    extends Product4[Shot[T1], Shot[T2], Shot[T3], Shot[T4]] with ShotsN {
    override def and[T5](_5: Shot[T5]): Shots5[T1, T2, T3, T4, T5] = Shots5(_1, _2, _3, _4, _5)

    def get: Shot[(T1, T2, T3, T4)] = (_1, _2, _3, _4) match {
      case (Hit(e1), Hit(e2), Hit(e3), Hit(e4)) => Hit((e1, e2, e3, e4))
      case _ => combinedMiss(productIterator)
    }

    def apply[R](f: (T1, T2, T3, T4) => R): Shot[R] = (_1, _2, _3, _4) match {
      case (Hit(e1), Hit(e2), Hit(e3), Hit(e4)) => Hit(f(e1, e2, e3, e4))
      case _ => combinedMiss(productIterator)
    }
  }

  case class Shots5[+T1, +T2, +T3, +T4, +T5](_1: Shot[T1], _2: Shot[T2], _3: Shot[T3], _4: Shot[T4], _5: Shot[T5])
    extends Product5[Shot[T1], Shot[T2], Shot[T3], Shot[T4], Shot[T5]] with ShotsN {
    override def and[T6](_6: Shot[T6]): Shots6[T1, T2, T3, T4, T5, T6] = Shots6(_1, _2, _3, _4, _5, _6)

    def get: Shot[(T1, T2, T3, T4, T5)] = (_1, _2, _3, _4, _5) match {
      case (Hit(e1), Hit(e2), Hit(e3), Hit(e4), Hit(e5)) => Hit((e1, e2, e3, e4, e5))
      case _ => combinedMiss(productIterator)
    }

    def apply[R](f: (T1, T2, T3, T4, T5) => R): Shot[R] = (_1, _2, _3, _4, _5) match {
      case (Hit(e1), Hit(e2), Hit(e3), Hit(e4), Hit(e5)) => Hit(f(e1, e2, e3, e4, e5))
      case _ => combinedMiss(productIterator)
    }
  }

  case class Shots6[+T1, +T2, +T3, +T4, +T5, +T6](_1: Shot[T1], _2: Shot[T2], _3: Shot[T3], _4: Shot[T4],
                                                  _5: Shot[T5], _6: Shot[T6])
    extends Product6[Shot[T1], Shot[T2], Shot[T3], Shot[T4], Shot[T5], Shot[T6]] with ShotsN {
    override def and[T7](_7: Shot[T7]): Shots7[T1, T2, T3, T4, T5, T6, T7] = Shots7(_1, _2, _3, _4, _5, _6, _7)

    def get: Shot[(T1, T2, T3, T4, T5, T6)] = (_1, _2, _3, _4, _5, _6) match {
      case (Hit(e1), Hit(e2), Hit(e3), Hit(e4), Hit(e5), Hit(e6)) => Hit((e1, e2, e3, e4, e5, e6))
      case _ => combinedMiss(productIterator)
    }

    def apply[R](f: (T1, T2, T3, T4, T5, T6) => R): Shot[R] = (_1, _2, _3, _4, _5, _6) match {
      case (Hit(e1), Hit(e2), Hit(e3), Hit(e4), Hit(e5), Hit(e6)) => Hit(f(e1, e2, e3, e4, e5, e6))
      case _ => combinedMiss(productIterator)
    }
  }

  def missingShots8ImplementationError: NotImplementedError =
    new NotImplementedError("If you want to combine more than seven shots, " +
      "you need to implement ShotCombinators.Shots8")

  case class Shots7[+T1, +T2, +T3, +T4, +T5, +T6, +T7](_1: Shot[T1], _2: Shot[T2], _3: Shot[T3], _4: Shot[T4],
                                                       _5: Shot[T5], _6: Shot[T6], _7: Shot[T7])
    extends Product7[Shot[T1], Shot[T2], Shot[T3], Shot[T4], Shot[T5], Shot[T6], Shot[T7]] with ShotsN {
    override def and[T8](_8: Shot[T8]): ShotsN = throw missingShots8ImplementationError

    def get: Shot[(T1, T2, T3, T4, T5, T6, T7)] = (_1, _2, _3, _4, _5, _6, _7) match {
      case (Hit(e1), Hit(e2), Hit(e3), Hit(e4), Hit(e5), Hit(e6), Hit(e7)) => Hit((e1, e2, e3, e4, e5, e6, e7))
      case _ => combinedMiss(productIterator)
    }
    def apply[R](f: (T1, T2, T3, T4, T5, T6, T7) => R): Shot[R] = (_1, _2, _3, _4, _5, _6, _7) match {
      case (Hit(e1), Hit(e2), Hit(e3), Hit(e4), Hit(e5), Hit(e6), Hit(e7)) => Hit(f(e1, e2, e3, e4, e5, e6, e7))
      case _ => combinedMiss(productIterator)
    }
  }

}
