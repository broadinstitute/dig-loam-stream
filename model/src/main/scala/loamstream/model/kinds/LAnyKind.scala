package loamstream.model.kinds

/**
  * LoamStream
  * Created by oliverr on 2/11/2016.
  */
case object LAnyKind extends LKind {
  override def isSubKindOf(oKind: LKind): Boolean = oKind == LAnyKind

  override def isSuperKindOf(oKind: LKind): Boolean = true
}
