package loamstream.model

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
object StoreSig {
  def create[T: TypeTag]: StoreSig = new StoreSig(typeTag[T].tpe)
}

final class StoreSig(val tpe: Type) {
  override def equals(o: Any): Boolean = o match {
    case oSig: StoreSig => tpe =:= oSig.tpe
    case _ => false
  }

  override def hashCode: Int = tpe.toString.hashCode

  def =:=(other: StoreSig): Boolean = tpe =:= other.tpe

  def <:<(other: StoreSig): Boolean = tpe <:< other.tpe

  def >:>(other: StoreSig): Boolean = other.tpe <:< tpe
}
