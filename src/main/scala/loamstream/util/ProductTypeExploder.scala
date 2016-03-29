package loamstream.util

import scala.reflect.runtime.universe.Type

/**
  * LoamStream
  * Created by oliverr on 1/20/2016.
  */
object ProductTypeExploder {

  private val tupleRegex = "scala.Tuple(\\d+)".r
  
  def explode(tpe: Type): Seq[Type] = {
    val fullName = tpe.typeConstructor.typeSymbol.fullName
    
    fullName match {
      case tupleRegex(_) => tpe.typeArgs
      case _ => Seq(tpe)
    }
  }
}
