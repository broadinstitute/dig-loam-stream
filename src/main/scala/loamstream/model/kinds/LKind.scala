package loamstream.model.kinds

/**
  * LoamStream
  * Created by oliverr on 2/11/2016.
  */
trait LKind {

  def <:<(oKind: LKind): Boolean

  final def isA(oKind: LKind): Boolean = <:<(oKind) 
  
  def >:>(oKind: LKind): Boolean
  
  final def hasSubKind(oKind: LKind): Boolean = >:>(oKind)
}
