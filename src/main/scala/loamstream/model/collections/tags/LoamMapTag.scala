package loamstream.model.collections.tags

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
object LoamMapTag {

  def fromKeys[T, P <: LoamKeyTag[_, _], V: TypeTag](keys: LoamKeyTag[T, P]): LoamMapTag[T, P, V] =
    LoamMapTag(keys, typeTag[V])

}

case class LoamMapTag[T, P <: LoamKeyTag[_, _], V](keys: LoamKeyTag[T, P], vTag: TypeTag[V])
  extends LoamHeapTag[T, P] {

  def keySet: LoamSetTag[T, P] = LoamSetTag(keys)

  override def toString: String = "LMap(" + keys + " -> " + vTag.tpe + ")"

  override def withKey[TC: TypeTag] = LoamMapTag(keys.key[TC], vTag)
}
