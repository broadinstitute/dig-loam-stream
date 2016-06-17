package loamstream.model

import scala.reflect.runtime.universe.Type

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
final class StoreSpec(val tpe: Type) {
  override def equals(o: Any): Boolean = o match {
    case oSig: StoreSpec => tpe =:= oSig.tpe
    case _ => false
  }

  override def hashCode: Int = tpe.toString.hashCode

  def =:=(other: StoreSpec): Boolean = tpe =:= other.tpe

  def <:<(other: StoreSpec): Boolean = tpe =:= other.tpe

  def >:>(other: StoreSpec): Boolean = tpe =:= other.tpe
}
