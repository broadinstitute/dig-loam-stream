package loamstream.model.kinds

/**
  * LoamStream
  * Created by oliverr on 2/11/2016.
  */
object LSpecificKind {
  def apply(specifics: Any, supers: LKind*): LSpecificKind = LSpecificKind(specifics, supers.toSet)
}

final case class LSpecificKind(specifics: Any, supers: Set[LKind]) extends LKind {
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
