package loamstream.model.kinds

/**
  * LoamStream
  * Created by oliverr on 2/11/2016.
  */
case object LNoKind extends LKind {
  override def <:<(oKind: LKind): Boolean = true

  override def >:>(oKind: LKind): Boolean = oKind == LNoKind
}
