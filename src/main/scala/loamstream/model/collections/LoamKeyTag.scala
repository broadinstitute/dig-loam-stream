package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
object LoamKeyTag {

  def createRoot[T]: LoamKeyTag[T, Nothing] = new LoamKeyTag[T, Nothing](None)

  def node[T] = createRoot[T]

}

class LoamKeyTag[T, P <: LoamKeyTag[_, _]](val parentOpt: Option[P]) {

  def createChild[TC]: LoamKeyTag[TC, this.type] = new LoamKeyTag[TC, this.type](Some(this))

  def node[TC] = createChild[TC]

}
