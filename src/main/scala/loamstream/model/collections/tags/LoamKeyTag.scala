package loamstream.model.collections.tags

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
object LoamKeyTag {

  def createRoot[T: TypeTag]: LoamKeyTag[T, Nothing] = new LoamKeyTag[T, Nothing](typeTag[T], None)

  def key[T: TypeTag] = createRoot[T]

}

object LKey {
  def key[T: TypeTag] = LoamKeyTag.createRoot[T]
}

case class LoamKeyTag[T, P <: LoamKeyTag[_, _]](tpeTag: TypeTag[T], parentOpt: Option[P]) {

  def createChild[TC: TypeTag]: LoamKeyTag[TC, LoamKeyTag[T, P]] =
    new LoamKeyTag[TC, LoamKeyTag[T, P]](typeTag[TC], Some(this))

  def key[TC: TypeTag] = createChild[TC]

  override def toString: String = parentOpt.map(_.toString).getOrElse("") + "/" + tpeTag.tpe

  def getLSet: LoamSetTag[T, P] = LoamSetTag(this)

  def getLMap[V: TypeTag]: LoamMapTag[T, P, V] = LoamMapTag.fromKeys[T, P, V](this)

}
