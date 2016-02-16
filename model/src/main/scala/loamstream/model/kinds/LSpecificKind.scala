package loamstream.model.kinds

/**
  * LoamStream
  * Created by oliverr on 2/11/2016.
  */
object LSpecificKind {
  def apply[T](specifics: T, supers: LKind*): LSpecificKind[T] = LSpecificKind[T](specifics, supers.toSet)
}

case class LSpecificKind[T](specifics: T, supers: Set[LKind]) extends LKind {
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
