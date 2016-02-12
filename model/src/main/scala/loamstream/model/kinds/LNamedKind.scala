package loamstream.model.kinds

/**
  * LoamStream
  * Created by oliverr on 2/11/2016.
  */
object LNamedKind {
  def apply(name: String, supers: LKind*) = LNamedKind(name, supers.toSet)
}

case class LNamedKind(name: String, supers: Set[LKind]) extends LKind {
  override def <:<(oKind: LKind): Boolean = oKind match {
    case _ if oKind == this => true
    case LAnyKind => true
    case LNoKind => false
    case _ => supers.map(_ <:< oKind).fold(false)(_ || _)
  }

  override def >:>(oKind: LKind): Boolean = oKind match {
    case _ if oKind == this => true
    case LNamedKind(_, oSupers) => oSupers.map(_ <:< this).fold(false)(_ || _)
    case LAnyKind => false
    case LNoKind => true
  }
}
