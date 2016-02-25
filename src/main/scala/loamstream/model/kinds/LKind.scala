package loamstream.model.kinds

/**
  * LoamStream
  * Created by oliverr on 2/11/2016.
  */
trait LKind {

  def <:<(oKind: LKind): Boolean

  def >:>(oKind: LKind): Boolean

}
