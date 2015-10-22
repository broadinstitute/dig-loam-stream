package loamstream.model.collections.tags

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
object LoamKeyTag {

  def createRoot[T: TypeTag]: LoamKeyTag[T, Nothing] = new LoamKeyTag[T, Nothing](typeTag[T], None)

  def node[T: TypeTag] = createRoot[T]

}

case class LoamKeyTag[T, P <: LoamKeyTag[_, _]](tpeTag: TypeTag[T], parentOpt: Option[P]) {

  def createChild[TC: TypeTag]: LoamKeyTag[TC, this.type] = new LoamKeyTag[TC, this.type](typeTag[TC], Some(this))

  def node[TC: TypeTag] = createChild[TC]

  override def toString: String = parentOpt.map(_.toString).getOrElse("") + "/" + tpeTag.tpe

}
