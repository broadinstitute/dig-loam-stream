package loamstream.util

import scala.reflect.runtime.universe.Type

/**
  * LoamStream
  * Created by oliverr on 1/20/2016.
  */
object ProductTypeExploder {

  val tuplePrefix = "scala.Tuple"

  def explode(tpe: Type): Seq[Type] = {
    val fullName = tpe.typeConstructor.typeSymbol.fullName
    if (fullName.startsWith(tuplePrefix)) {
      val suffix = fullName.replace(tuplePrefix, "")
      try {
        suffix.toInt
        tpe.typeArgs
      } catch {
        case ex: NumberFormatException => Seq(tpe)
      }
    } else {
      Seq(tpe)
    }
  }

}
