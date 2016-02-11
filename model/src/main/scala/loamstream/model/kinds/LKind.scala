package loamstream.model.kinds

/**
  * LoamStream
  * Created by oliverr on 2/11/2016.
  */
trait LKind {

  def isSubKindOf(oKind: LKind): Boolean

  def isSuperKindOf(oKind: LKind): Boolean

}
