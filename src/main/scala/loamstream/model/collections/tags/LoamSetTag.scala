package loamstream.model.collections.tags

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
case class LoamSetTag[T, P <: LoamKeyTag[_, _]](keys: LoamKeyTag[T, P]) extends LoamHeapTag[T, P] {
  override def toString: String = "LSet(" + keys + ")"

  override def withKey[TC: TypeTag]: LoamSetTag[TC, LoamKeyTag[T, P]] = LoamSetTag(keys.key[TC])
}
