package loamstream.model.kinds

/**
  * LoamStream
  * Created by oliverr on 2/11/2016.
  */
object LNamedKind {
  def apply(name: String, supers: LKind*) = LNamedKind(name, supers.toSet)
}

case class LNamedKind(name: String, supers: Set[LKind]) extends LKind {
  override def isSubKindOf(oKind: LKind): Boolean = ???

  override def isSuperKindOf(oKind: LKind): Boolean = ???
}
