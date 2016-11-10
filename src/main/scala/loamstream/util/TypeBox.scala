package loamstream.util

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 6/9/16.
  */
object TypeBox {

  trait Untyped {
    def tpe: Type

    def isSubTypeOf(oBox: TypeBox.Untyped): Boolean = this <:< oBox

    def <:<(oBox: TypeBox.Untyped): Boolean = tpe <:< oBox.tpe

    def isSuperTypeOf(oBox: TypeBox.Untyped): Boolean = this >:> oBox

    def >:>(oBox: TypeBox.Untyped): Boolean = oBox.tpe <:< tpe

    override def equals(o: Any): Boolean = o match {
      case oBox: TypeBox.Untyped => tpe =:= oBox.tpe
      case _ => false
    }

    override def hashCode: Int = tpe.toString.hashCode

  }

  def of[A: TypeTag]: TypeBox[A] = new TypeBox[A](typeTag[A].tpe)
}

final class TypeBox[A](val tpe: Type) extends TypeBox.Untyped {

}
