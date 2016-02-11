package loamstream.model.kinds

/**
  * LoamStream
  * Created by oliverr on 2/11/2016.
  */
case object LNoKind extends LKind {
  override def isSubKindOf(oKind: LKind): Boolean = true

  override def isSuperKindOf(oKind: LKind): Boolean = oKind == LNoKind
}
