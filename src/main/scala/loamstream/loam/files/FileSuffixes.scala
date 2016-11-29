package loamstream.loam.files


import loamstream.loam.ops.StoreType

import scala.reflect.runtime.universe.{Type, typeOf}

/**
  * LoamStream
  * Created by oliverr on 6/17/2016.
  */
object FileSuffixes {

  def apply(tpe: Type): String = {
    if (tpe =:= typeOf[StoreType.VCF]) {
      "vcf"
    } else {
      "txt"
    }
  }

}
