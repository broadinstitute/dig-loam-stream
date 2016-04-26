package loamstream.util

import loamstream.util.shot.{Hit, Miss, Shot}

/**
  * LoamStream
  * Created by oliverr on 4/26/2016.
  */
object TupleUtil {

  def seqToProduct[T](seq: Seq[T]): Shot[Product] = {
    val size = seq.size
    size match {
      case 1 => Hit(Tuple1(seq(0)))
      case 2 => Hit(Tuple2(seq(0), seq(1)))
      case _ => Miss(s"Don't know how to convert Seq of size ${size} to product.")
    }
  }

}
