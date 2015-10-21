package loamstream.model.collections

import scala.reflect.runtime.universe.{TypeTag, typeOf}

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
object LoamKeyTag {

  def createRoot[T: TypeTag]: LoamKeyTag[T, Nothing] = new LoamKeyTag[T, Nothing](None)

  def node[T: TypeTag] = createRoot[T]

}

class LoamKeyTag[T: TypeTag, P <: LoamKeyTag[_, _]](val parentOpt: Option[P]) {

  def createChild[TC: TypeTag]: LoamKeyTag[TC, this.type] = new LoamKeyTag[TC, this.type](Some(this))

  def node[TC: TypeTag] = createChild[TC]

  override def toString: String = parentOpt.map(_.toString).getOrElse("") + "/" + typeOf[T]

}
