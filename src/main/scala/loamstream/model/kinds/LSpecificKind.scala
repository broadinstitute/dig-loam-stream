package loamstream.model.kinds

import loamstream.model.values.LType.LString
import loamstream.model.values.LValue

/**
  * LoamStream
  * Created by oliverr on 2/11/2016.
  */
object LSpecificKind {
  def apply(specifics: String, supers: LKind*): LSpecificKind[String] = LSpecificKind(LString(specifics), supers.toSet)

  def apply[T](specifics: LValue[T], supers: LKind*): LSpecificKind[T] = LSpecificKind[T](specifics, supers.toSet)
}

case class LSpecificKind[T](specifics: LValue[T], supers: Set[LKind]) extends LKind {
  override def toString: String = {
    s"LSpecificKind($specifics extends ${supers.mkString(",")})"
  }
  
  override def <:<(oKind: LKind): Boolean = oKind match {
    case _ if oKind == this => true
    case LAnyKind => true
    case LNoKind => false
    case _ => supers.map(_ <:< oKind).fold(false)(_ || _)
  }

  override def >:>(oKind: LKind): Boolean = oKind match {
    case _ if oKind == this => true
    case LSpecificKind(_, oSupers) => oSupers.map(_ <:< this).fold(false)(_ || _)
    case LAnyKind => false
    case LNoKind => true
  }
}
