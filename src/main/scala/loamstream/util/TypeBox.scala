package loamstream.util

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 6/9/16.
  */
object TypeBox {
  def of[A: TypeTag]: TypeBox[A] = new TypeBox[A](typeTag[A].tpe)
}

final class TypeBox[A](val tpe: Type) {
  
  def isSubTypeOf[B](oBox: TypeBox[B]): Boolean = this <:< oBox
  def <:<[B](oBox: TypeBox[B]): Boolean = tpe <:< oBox.tpe

  def isSuperTypeOf[B](oBox: TypeBox[B]): Boolean = this >:> oBox 
  def >:>[B](oBox: TypeBox[B]): Boolean = oBox.tpe <:< tpe

  override def equals(o: Any): Boolean = o match {
    case oBox: TypeBox[_] => tpe =:= oBox.tpe
    case _ => false
  }
  
  override def hashCode: Int = tpe.toString.hashCode
}
