package loamstream.model.kinds

/**
  * LoamStream
  * Created by oliverr on 2/11/2016.
  */
case object LAnyKind extends LKind {
  override def <:<(oKind: LKind): Boolean = oKind == LAnyKind

  override def >:>(oKind: LKind): Boolean = true
}
