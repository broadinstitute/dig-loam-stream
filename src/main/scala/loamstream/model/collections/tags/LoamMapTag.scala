package loamstream.model.collections.tags

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
object LoamMapTag {

  def createMap[K <: LoamKeyTag[_, _], V: TypeTag](keys: K): LoamMapTag[K, V] = LoamMapTag(keys, typeTag[V])

}

case class LoamMapTag[K <: LoamKeyTag[_, _], V](keys: K, vTag: TypeTag[V]) extends LoamHeapTag[K] {

  def keySet: LoamSetTag[K] = LoamSetTag(keys)

  override def toString: String = "LMap(" + keys + " -> " + vTag.tpe + ")"

}
