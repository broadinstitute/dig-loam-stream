package loamstream.util

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 6/9/16.
  */
object TypeBox {
  def of[T: TypeTag]: TypeBox[T] = new TypeBox[T](typeTag[T].tpe)
}

final class TypeBox[T](val tpe: Type) {
  override def equals(o: Any): Boolean = o match {
    case oBox: TypeBox[_] => tpe =:= oBox.tpe
    case _ => false
  }

  //TODO: Describe what these operators mean, possibly provide a human-language alternative
  def <:<[S](oBox: TypeBox[S]): Boolean = tpe <:< oBox.tpe

  //TODO: Describe what these operators mean, possibly provide a human-language alternative
  def >:>[S](oBox: TypeBox[S]): Boolean = oBox.tpe <:< tpe

  override def hashCode: Int = tpe.toString.hashCode
}
