package loamstream.util

import loamstream.util.shot.{Hit, Miss, Shot}

/**
  * LoamStream
  * Created by oliverr on 4/26/2016.
  */
object TupleUtil {

  // scalastyle:off cyclomatic.complexity
  def seqToProduct[T](seq: Seq[T]): Shot[Product] = {
    seq match {
      case Seq() => Hit(None)
      case Seq(e1) => Hit(Tuple1(e1))
      case Seq(e1, e2) => Hit((e1, e2))
      case Seq(e1, e2, e3) => Hit((e1, e2, e3))
      case Seq(e1, e2, e3, e4) => Hit((e1, e2, e3, e4))
      case Seq(e1, e2, e3, e4, e5) => Hit((e1, e2, e3, e4, e5))
      case Seq(e1, e2, e3, e4, e5, e6) => Hit((e1, e2, e3, e4, e5, e6))
      case Seq(e1, e2, e3, e4, e5, e6, e7) => Hit((e1, e2, e3, e4, e5, e6, e7))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8) => Hit((e1, e2, e3, e4, e5, e6, e7, e8))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9) => Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10) => Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11) => Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12) =>
        Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13) =>
        Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14) =>
        Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15) =>
        Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16) =>
        Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17) =>
        Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18) =>
        Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19) =>
        Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20) =>
        Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21) =>
        Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22) =>
        Hit((e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22))
      case _ if seq.isEmpty => Miss("Can not convert empty Seq to Product.")
      case _ => Miss(s"Don't know how to convert Seq of size ${seq.size} to product.")
    }
  }

  // scalastyle:on cyclomatic.complexity

}
